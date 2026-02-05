package server

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"wicos64-server/internal/config"
	"wicos64-server/internal/version"
)

// WDP1 UDP LAN discovery.
//
// Request: 24 bytes
//
//	0..3  magic  "WDP1"
//	4     type   0x01 (DISCOVER)
//	5     flags  (bit1 = bootstrap-only)
//	6..7  seq    u16 LE
//	8..11 nonce  u32 LE
//	12..17 mac   6 bytes (raw)
//	18..19 listen_port u16 LE (client expects reply here)
//	20..23 crc32 u32 LE over bytes 0..19
//
// Response: OFFER, 32 bytes (no optional strings for now)
//
//	0..3  magic  "WDP1"
//	4     type   0x02 (OFFER)
//	5     flags  (bit0 bootstrap enabled, bit2 requires cfg token, bit3 supports POST bootstrap)
//	6..7  seq    u16 LE (mirrored)
//	8..11 nonce  u32 LE (mirrored)
//	12..15 server_ip IPv4 (network order)
//	16..17 http_port u16 LE
//	18     api_path_len u8 (0 => client default)
//	19     bootstrap_path_len u8 (0 => client default)
//	20..21 server_version u16 LE (major<<8|minor)
//	22..23 caps_flags u16 LE (reserved for future use)
//	24..27 server_id u32 LE (CRC32(server_name))
//	28..31 crc32 u32 LE over bytes 0..27
const (
	wdpMagic          = "WDP1"
	wdpTypeDiscover   = 0x01
	wdpTypeOffer      = 0x02
	wdpReqSize        = 24
	wdpOfferFixedSize = 32

	wdpReqFlagBootstrapOnly = 1 << 1
)

type udpRateLimiter struct {
	mu        sync.Mutex
	windowSec int64
	counts    map[string]int
}

func newUDPRateLimiter() *udpRateLimiter {
	return &udpRateLimiter{counts: map[string]int{}}
}

func (rl *udpRateLimiter) allow(ip string, limit int) bool {
	if limit <= 0 {
		return true
	}
	nowSec := time.Now().Unix()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.windowSec != nowSec {
		rl.windowSec = nowSec
		for k := range rl.counts {
			delete(rl.counts, k)
		}
	}
	rl.counts[ip]++
	return rl.counts[ip] <= limit
}

// StartDiscovery starts the UDP listener once (best-effort). It never panics.
func (s *Server) StartDiscovery() {
	s.discOnce.Do(func() {
		cfg := s.cfgSnapshot()
		dc := cfg.Discovery
		if !dc.Enabled {
			return
		}
		addr := &net.UDPAddr{IP: net.IPv4zero, Port: dc.UDPPort}
		conn, err := net.ListenUDP("udp4", addr)
		if err != nil {
			log.Printf("UDP discovery: listen %s failed: %v", addr.String(), err)
			return
		}
		log.Printf("UDP discovery: listening on %s (LAN only=%v)", addr.String(), dc.LanOnly)

		rl := newUDPRateLimiter()
		go s.discoveryLoop(conn, rl)
	})
}

func (s *Server) discoveryLoop(conn *net.UDPConn, rl *udpRateLimiter) {
	buf := make([]byte, 2048)
	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("UDP discovery: read error: %v", err)
			continue
		}
		if n != wdpReqSize {
			continue
		}
		pkt := buf[:n]
		if string(pkt[0:4]) != wdpMagic {
			continue
		}
		if pkt[4] != wdpTypeDiscover {
			continue
		}

		cfg := s.cfgSnapshot()
		dc := cfg.Discovery
		if !dc.Enabled {
			continue
		}
		if dc.LanOnly {
			if !isLANIP(src.IP) {
				continue
			}
		}
		if !rl.allow(src.IP.String(), dc.RateLimitPerSec) {
			continue
		}

		// CRC32 over bytes 0..19.
		want := binary.LittleEndian.Uint32(pkt[20:24])
		got := crc32.ChecksumIEEE(pkt[0:20])
		if got != want {
			log.Printf("UDP discovery: DISCOVER crc fail from %s", src.IP.String())
			continue
		}

		flagsReq := pkt[5]
		seq := binary.LittleEndian.Uint16(pkt[6:8])
		nonce := binary.LittleEndian.Uint32(pkt[8:12])
		macRaw := pkt[12:18]
		listenPort := binary.LittleEndian.Uint16(pkt[18:20])

		// Optional: If client requests bootstrap-only and server bootstrap is disabled, do not answer.
		if (flagsReq&wdpReqFlagBootstrapOnly) != 0 && !cfg.Bootstrap.Enabled {
			continue
		}

		macStr := fmt.Sprintf("%02X%02X%02X%02X%02X%02X", macRaw[0], macRaw[1], macRaw[2], macRaw[3], macRaw[4], macRaw[5])

		// Optional: If bootstrap is enabled and unknown MAC policy is "deny", only answer
		// if the MAC is explicitly allowed.
		if cfg.Bootstrap.Enabled {
			pol := strings.ToLower(strings.TrimSpace(cfg.Bootstrap.UnknownMACPolicy))
			if pol == "deny" {
				if _, ok := cfg.Bootstrap.MacTokens[macStr]; !ok {
					continue
				}
			}
		}

		offer, flagsOffer, caps, sid, srvIP, httpPort := buildWDP1Offer(cfg, src.IP, seq, nonce)
		// reply: unicast to src.IP but client-chosen port.
		_, werr := conn.WriteToUDP(offer, &net.UDPAddr{IP: src.IP, Port: int(listenPort)})
		if werr != nil {
			log.Printf("UDP discovery: OFFER send failed to %s:%d: %v", src.IP.String(), listenPort, werr)
			continue
		}
		log.Printf("UDP discovery: DISCOVER ok from %s:%d mac=%s -> OFFER %s:%d flags=0x%02x caps=0x%04x id=0x%08x", src.IP.String(), listenPort, macStr, srvIP.String(), httpPort, flagsOffer, caps, sid)
	}
}

func buildWDP1Offer(cfg config.Config, clientIP net.IP, seq uint16, nonce uint32) (offer []byte, flags byte, caps uint16, serverID uint32, serverIP net.IP, httpPort int) {
	offer = make([]byte, wdpOfferFixedSize)
	copy(offer[0:4], []byte(wdpMagic))
	offer[4] = wdpTypeOffer

	flags = 0
	if cfg.Bootstrap.Enabled {
		flags |= 1 << 0
		if strings.TrimSpace(cfg.Bootstrap.Token) != "" {
			flags |= 1 << 2
		}
		if cfg.Bootstrap.AllowPOST {
			flags |= 1 << 3
		}
	}
	offer[5] = flags

	binary.LittleEndian.PutUint16(offer[6:8], seq)
	binary.LittleEndian.PutUint32(offer[8:12], nonce)

	serverIP = advertisedServerIP(cfg.Listen, clientIP)
	if ip4 := serverIP.To4(); ip4 != nil {
		copy(offer[12:16], ip4)
	} else {
		copy(offer[12:16], net.IPv4(127, 0, 0, 1))
	}

	httpPort = listenHTTPPort(cfg.Listen)
	binary.LittleEndian.PutUint16(offer[16:18], uint16(httpPort))

	// Optional strings: keep empty for robustness (client defaults).
	offer[18] = 0 // api_path_len
	offer[19] = 0 // bootstrap_path_len

	verU16 := serverVersionU16(version.Get().Version)
	binary.LittleEndian.PutUint16(offer[20:22], verU16)

	caps = 0
	binary.LittleEndian.PutUint16(offer[22:24], caps)

	serverID = crc32.ChecksumIEEE([]byte(cfg.ServerName))
	binary.LittleEndian.PutUint32(offer[24:28], serverID)

	crc := crc32.ChecksumIEEE(offer[0:28])
	binary.LittleEndian.PutUint32(offer[28:32], crc)
	return offer, flags, caps, serverID, serverIP, httpPort
}

func listenHTTPPort(listen string) int {
	_, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		// If listen is just ":8080" SplitHostPort works; if not, fall back.
		if strings.HasPrefix(listen, ":") {
			p, _ := strconv.Atoi(strings.TrimPrefix(listen, ":"))
			if p > 0 {
				return p
			}
		}
		return 8080
	}
	p, err := strconv.Atoi(portStr)
	if err != nil || p <= 0 {
		return 8080
	}
	return p
}

func advertisedServerIP(listen string, clientIP net.IP) net.IP {
	// If the server is bound to a specific non-loopback IPv4 address, prefer it.
	host, _, err := net.SplitHostPort(listen)
	if err == nil {
		host = strings.TrimSpace(host)
		if host != "" {
			// If it's an IP literal.
			if ip := net.ParseIP(host); ip != nil {
				// If the server only listens on loopback but the client is remote, try to find a better IP.
				if ip.IsLoopback() && clientIP != nil && !clientIP.IsLoopback() {
					return outboundIPToClient(clientIP)
				}
				if ip4 := ip.To4(); ip4 != nil {
					if !ip4.Equal(net.IPv4zero) {
						return ip4
					}
				}
			} else {
				// Hostname: try to resolve.
				ips, _ := net.LookupIP(host)
				for _, ip := range ips {
					if ip4 := ip.To4(); ip4 != nil {
						return ip4
					}
				}
			}
		}
	}
	// Default: choose the outbound interface IP we'd use to reach the client.
	if clientIP != nil {
		return outboundIPToClient(clientIP)
	}
	return net.IPv4(127, 0, 0, 1)
}

func outboundIPToClient(dst net.IP) net.IP {
	if dst == nil {
		return net.IPv4(127, 0, 0, 1)
	}
	// Dial UDP to discover the local outbound interface.
	c, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: dst, Port: 9})
	if err != nil {
		return net.IPv4(127, 0, 0, 1)
	}
	defer c.Close()
	la, ok := c.LocalAddr().(*net.UDPAddr)
	if !ok {
		return net.IPv4(127, 0, 0, 1)
	}
	if ip4 := la.IP.To4(); ip4 != nil {
		return ip4
	}
	return net.IPv4(127, 0, 0, 1)
}

func serverVersionU16(ver string) uint16 {
	// Expect formats like "v1.0.1.6" or "1.0.1".
	ver = strings.TrimSpace(ver)
	ver = strings.TrimPrefix(ver, "v")
	parts := strings.Split(ver, ".")
	if len(parts) < 2 {
		return 0
	}
	maj, _ := strconv.Atoi(parts[0])
	min, _ := strconv.Atoi(parts[1])
	if maj < 0 || maj > 255 || min < 0 || min > 255 {
		return 0
	}
	return uint16((maj << 8) | min)
}

package rsync

import ()

const (
	// mod is the largest prime that is less than 65536.
	mod = 65521
	// nmax is the largest n such that
	// 255 * n * (n+1) / 2 + (n+1) * (mod-1) <= 2^32-1.
	// It is mentioned in RFC 1950 (search for "5552").
	nmax = 5552
)

func newWeakChecksum() *weakChecksum {
	d := new(weakChecksum)
	d.Reset()
	return d
}

// weakChecksum is a rolling hash implementation of the adler32.
type weakChecksum struct {
	digest         uint32
	data           [BlockSize]byte
	firstByteIndex int
	n              int
}

func (d *weakChecksum) Reset() {
	d.digest = 1
	d.n = 0
	d.firstByteIndex = 0
}

func (d *weakChecksum) Size() int { return 4 }

func (d *weakChecksum) BlockSize() int { return 1 }

func (d *weakChecksum) Write(p []byte) (int, error) {
	if d.n == BlockSize {
		// roll one byte
		d.rollWeakChecksum(d.data[d.firstByteIndex], p[0])
		d.data[d.firstByteIndex] = p[0]
		d.firstByteIndex = (d.firstByteIndex + 1) % BlockSize
		return 1, nil
	} else {
		canAdd := BlockSize - d.n
		if canAdd > len(p) {
			copy(d.data[d.n:d.n+len(p)], p)
			d.n += len(p)
			d.addData(p...)
			return len(p), nil
		} else {
			copy(d.data[d.n:BlockSize], p[0:canAdd])
			d.n += canAdd
			d.addData(p...)
			return canAdd, nil
		}
	}
}

func (d *weakChecksum) Sum32() uint32 { return d.digest }

func (d *weakChecksum) Sum(in []byte) []byte {
	s := d.digest
	return append(in, byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}

// getWeakChecksum returns the Adler-32 checksum of data.
func getWeakChecksum(data []byte) uint32 {
	d := newWeakChecksum()
	d.addData(data...)
	return d.digest
}

// addData add p to the running checksum d.
func (d *weakChecksum) addData(p ...byte) {
	s1, s2 := d.digest&0xffff, d.digest>>16
	for len(p) > 0 {
		var q []byte
		if len(p) > nmax {
			p, q = p[:nmax], p[nmax:]
		}
		for _, x := range p {
			s1 += uint32(x)
			s2 += s1
		}
		s1 %= mod
		s2 %= mod
		p = q
	}

	d.digest = (s2<<16 | s1)
}

func (d *weakChecksum) rollWeakChecksum(oldByte byte, newByte byte) {
	s1, s2 := d.digest&0xffff, d.digest>>16
	s1 += mod + uint32(newByte) - uint32(oldByte)
	s2 += mod + s1 - ((BlockSize)*uint32(oldByte))%mod - 1
	s1 %= mod
	s2 %= mod

	d.digest = (s2<<16 | s1)
}

package gossip

import (
	"math"
	"time"
)

const sampleSize = 1000

type detector struct {
	utime    time.Time
	interval time.Duration
	samples  []int64
	pos      int
}

func newDetector(interval time.Duration) *detector {
	return &detector{
		interval: interval,
		samples:  make([]int64, 0, sampleSize),
	}
}

func (d *detector) add(t time.Time) {
	var i int64

	if d.utime.IsZero() {
		i = int64(d.interval) / 2
	} else {
		i = int64(t.Sub(d.utime))
	}

	d.utime = t
	if len(d.samples) < sampleSize {
		d.samples = append(d.samples, i)
	} else {
		d.samples[d.pos] = i
	}

	d.pos++
	if d.pos >= len(d.samples) {
		d.pos = 0
	}
}

func (d *detector) mean() float64 {
	var sum int64
	for _, v := range d.samples {
		sum += v
	}

	return float64(sum) / float64(len(d.samples))
}

/**
 * P = E ^ (-1 * (now - lastTimestamp) / mean)
 * φ = - log10 P
 */
func (d *detector) phi(t time.Time) float64 {
	i := float64(t.Sub(d.utime))
	exp := -1 * i / d.mean()
	p := math.Pow(math.E, exp)

	return -1 * math.Log(p) / math.Log(10)
}

func (d *detector) reset() {
	d.utime = time.Time{}
	d.pos = 0
}

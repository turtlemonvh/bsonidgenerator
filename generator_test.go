package mongoidgenerator

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"labix.org/v2/mgo/bson"
	"testing"
	"time"
)

var (
	testTime = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
)

func TestInvalidConfigurations(t *testing.T) {
	var err error

	_, err = NewGenerator(testTime, 2<<25, 1, 2)
	if assert.Error(t, err) {
		assert.Equal(t, ErrNMachinesTooLarge, err)
	}

	_, err = NewGenerator(testTime, 2, 1, 2<<25)
	if assert.Error(t, err) {
		assert.Equal(t, ErrNItemsPerProcessTooLarge, err)
	}
}

func TestExpectedCount(t *testing.T) {
	var err error
	var c GeneratorConfig

	t.Run("Test 4,4,10", func(t *testing.T) {
		c, err = NewGenerator(testTime, 4, 4, 10)
		if assert.NoError(t, err) {
			ochan := make(chan bson.ObjectId, 1)
			go c.SendOnChannel(ochan)

			stats := examineChannel(t, ochan)
			assert.Equal(t, 160, stats.ntotal)
			assert.Equal(t, 4, stats.nmachines)
			assert.Equal(t, 4, stats.nprocs)
			assert.Equal(t, 10, stats.nincs)
			assert.Equal(t, 1, stats.ntimes)
		}
	})

	t.Run("Test 4,0,10", func(t *testing.T) {
		c, err = NewGenerator(testTime, 4, 0, 10)
		if assert.NoError(t, err) {
			ochan := make(chan bson.ObjectId, 1)
			go c.SendOnChannel(ochan)

			stats := examineChannel(t, ochan)
			assert.Equal(t, 0, stats.ntotal)
			assert.Equal(t, 0, stats.nmachines)
			assert.Equal(t, 0, stats.nprocs)
			assert.Equal(t, 0, stats.nincs)
			assert.Equal(t, 0, stats.ntimes)
		}
	})

	t.Run("Test 1,1,500", func(t *testing.T) {
		c, err = NewGenerator(testTime, 1, 1, 500)
		if assert.NoError(t, err) {
			ochan := make(chan bson.ObjectId, 1)
			go c.SendOnChannel(ochan)

			stats := examineChannel(t, ochan)
			assert.Equal(t, 500, stats.ntotal)
			assert.Equal(t, 1, stats.nmachines)
			assert.Equal(t, 1, stats.nprocs)
			assert.Equal(t, 500, stats.nincs)
			assert.Equal(t, 1, stats.ntimes)
		}
	})
}

type IdChannelStats struct {
	ntotal    int
	nmachines int
	nprocs    int
	nincs     int
	ntimes    int
}

func examineChannel(t *testing.T, oidChan <-chan bson.ObjectId) IdChannelStats {
	machines := make(map[uint32]bool)
	procs := make(map[uint16]bool)
	incs := make(map[int32]bool)
	times := make(map[time.Time]bool)

	// Drain channel and track stats
	var totalItems int
	for oid := range oidChan {
		totalItems++
		machineBts := oid.Machine()
		machines[binary.BigEndian.Uint32([]byte{0, machineBts[1], machineBts[1], machineBts[2]})] = true
		procs[oid.Pid()] = true
		incs[oid.Counter()] = true
		times[oid.Time()] = true
	}

	return IdChannelStats{
		ntotal:    totalItems,
		nmachines: len(machines),
		nprocs:    len(procs),
		nincs:     len(incs),
		ntimes:    len(times),
	}
}

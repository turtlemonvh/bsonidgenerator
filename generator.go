package bsonidgenerator

import (
	"encoding/binary"
	"fmt"
	"labix.org/v2/mgo/bson"
	"time"
)

type GeneratorConfig struct {
	Time                 time.Time // We just use down to the seconds
	Nmachines            uint32    // Holds 4 bytes, we only have 3
	NprocessesPerMachine uint16    // Holds 2 bytes, we have 2
	NitemsPerProcess     uint32    // Holds 4 bytes, we only have 3
}

var (
	ErrNMachinesTooLarge        = fmt.Errorf("Can only manage up to %d unique machines", 1<<24)
	ErrNItemsPerProcessTooLarge = fmt.Errorf("Can only manage up to %d items per process", 1<<24)
)

func NewGenerator(t time.Time, nmachines uint32, nproc uint16, ninc uint32) (GeneratorConfig, error) {
	c := GeneratorConfig{
		Time:                 t,
		Nmachines:            nmachines,
		NprocessesPerMachine: nproc,
		NitemsPerProcess:     ninc,
	}
	return c, c.Validate()
}

func (conf GeneratorConfig) Validate() error {
	if conf.Nmachines > 1<<24 {
		return ErrNMachinesTooLarge
	}
	if conf.NitemsPerProcess > 1<<24 {
		return ErrNItemsPerProcessTooLarge
	}
	return nil
}

// Generate a stream of object ids.
// Returns an error if the configuration doesn't validate.
// This function closes the channel when it has finished sending.
func (conf GeneratorConfig) SendOnChannel(oidChan chan<- bson.ObjectId) error {
	var err error
	if err = conf.Validate(); err != nil {
		return err
	}

	// Starttime
	stime := uint32(conf.Time.Unix())

	for machine := uint32(0); machine < conf.Nmachines; machine++ {
		for pid := uint16(0); pid < conf.NprocessesPerMachine; pid++ {
			for inc := uint32(0); inc < conf.NitemsPerProcess; inc++ {
				// From: http://bazaar.launchpad.net/+branch/mgo/v2/view/head:/bson/bson.go#L218
				var b [12]byte
				// Timestamp, 4 bytes
				binary.BigEndian.PutUint32(b[:], stime)
				// Machine, 3 bytes
				b[4] = byte(machine >> 16)
				b[5] = byte(machine >> 8)
				b[6] = byte(machine)
				// Pid, 2 bytes
				b[7] = byte(pid >> 8)
				b[8] = byte(pid)
				// Increment, 3 bytes
				b[9] = byte(inc >> 16)
				b[10] = byte(inc >> 8)
				b[11] = byte(inc)
				// Send on channel
				oidChan <- bson.ObjectId(b[:])
			}
		}
	}

	close(oidChan)

	return nil
}

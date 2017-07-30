package bsonidgenerator

import (
	"encoding/binary"
	"fmt"
	"labix.org/v2/mgo/bson"
	"time"
)

type Config struct {
	Time                 time.Time // We just use down to the seconds
	Nmachines            uint32    // Holds 4 bytes, we only have 3
	NprocessesPerMachine uint16    // Holds 2 bytes, we have 2
	NitemsPerProcess     uint32    // Holds 4 bytes, we only have 3
}

var (
	ErrNMachinesTooLarge        = fmt.Errorf("Can only manage up to %d unique machines", 1<<24)
	ErrNItemsPerProcessTooLarge = fmt.Errorf("Can only manage up to %d items per process", 1<<24)
)

func NewGenerator(t time.Time, nmachines uint32, nproc uint16, ninc uint32) (Config, error) {
	c := Config{
		Time:                 t,
		Nmachines:            nmachines,
		NprocessesPerMachine: nproc,
		NitemsPerProcess:     ninc,
	}
	return c, c.Validate()
}

// Checks that a Config is valid
func (conf Config) Validate() error {
	if conf.Nmachines > 1<<24 {
		return ErrNMachinesTooLarge
	}
	if conf.NitemsPerProcess > 1<<24 {
		return ErrNItemsPerProcessTooLarge
	}
	return nil
}

// Returns the number of ObjectIds that will be produced by this Config
func (conf Config) Count() int {
	return int(conf.Nmachines) * int(conf.NprocessesPerMachine) * int(conf.NitemsPerProcess)
}

// Creates object ids using the Config, placing them in a slice.
// Returns an error if the configuration doesn't validate.
func (conf Config) Generate() ([]bson.ObjectId, error) {
	oids := make([]bson.ObjectId, conf.Count())
	var err error
	if err = conf.Validate(); err != nil {
		return oids, err
	}

	ntotal := 0
	t := uint32(conf.Time.Unix())
	for machine := uint32(0); machine < conf.Nmachines; machine++ {
		for pid := uint16(0); pid < conf.NprocessesPerMachine; pid++ {
			for inc := uint32(0); inc < conf.NitemsPerProcess; inc++ {
				oids[ntotal] = CreateObjectId(t, machine, pid, inc)
				ntotal++
			}
		}
	}

	return oids, err
}

// Generate a stream of object ids using the Config.
// Returns an error if the configuration doesn't validate.
// This function closes the channel when it has finished sending.
func (conf Config) SendOnChannel(oidChan chan<- bson.ObjectId) error {
	defer close(oidChan)

	var err error
	if err = conf.Validate(); err != nil {
		return err
	}

	t := uint32(conf.Time.Unix())
	for machine := uint32(0); machine < conf.Nmachines; machine++ {
		for pid := uint16(0); pid < conf.NprocessesPerMachine; pid++ {
			for inc := uint32(0); inc < conf.NitemsPerProcess; inc++ {
				oidChan <- CreateObjectId(t, machine, pid, inc)
			}
		}
	}

	return nil
}

// Create an objct id from the set of primitives needed to seed its state
// From: http://bazaar.launchpad.net/+branch/mgo/v2/view/head:/bson/bson.go#L218
func CreateObjectId(t uint32, machine uint32, pid uint16, inc uint32) bson.ObjectId {
	var b [12]byte
	// Timestamp, 4 bytes
	binary.BigEndian.PutUint32(b[:], t)
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
	return bson.ObjectId(b[:])
}

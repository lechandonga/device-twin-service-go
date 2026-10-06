package twin

import "fmt"

// Delivery is one command handed to a live device connection.
type Delivery = Command

// Connection is a local, in-process stand-in for the platform<->device link.
// It carries no real network: commands travel over a buffered Go channel. The
// device side reports back through Service.Report.
type Connection struct {
	id    string
	devID string
	// Ch receives pending commands in per-device sequence order on connect
	// and reconnect. Buffer overflows never block the service: a full channel
	// leaves the command pending for the next redelivery attempt, so one slow
	// device cannot stall other devices.
	Ch      <-chan *Command
	ch      chan *Command
	service *Service
}

// ID returns the connection id used for duplicate-connect deduplication.
func (c *Connection) ID() string { return c.id }

// DeviceID returns the device this connection belongs to.
func (c *Connection) DeviceID() string { return c.devID }

// Close tells the service that this link went away. It is idempotent; closing
// a stale connection never tears down a newer one for the same device.
func (c *Connection) Close() error {
	return c.service.disconnect(c.devID, c.id)
}

// String renders a short handle for logs.
func (c *Connection) String() string {
	return fmt.Sprintf("conn[%s/%s]", c.devID, c.id)
}

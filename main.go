package main

import (
	"context"
	"fmt"
	"log"
	"maps"
	"sync"
	"time"
)

type FleetManager interface {
	AddTruck(id string, cargo int) error
	GetTruck(id string) (Truck, error)
	RemoveTruck(id string) error
	UpdateTruckCargo(id string, cargo int) error
}

func (m *truckManager) AddTruck(id string, cargo int) error {
	m.Lock()
	defer m.Unlock()
	m.trucks[id] = &Truck{Id: id, Cargo: cargo}
	return nil
}

func (m *truckManager) GetTruck(id string) (Truck, error) {
	m.RLock()
	defer m.RUnlock()
	truck, ok := m.trucks[id]
	if !ok {
		return Truck{}, fmt.Errorf("truck not found")
	}
	fmt.Printf("get: %v\n", truck)
	return *truck, nil
}

func (m *truckManager) RemoveTruck(id string) error {
	m.Lock()
	defer m.Unlock()
	delete(m.trucks, id)
	return nil
}

func (m *truckManager) UpdateTruckCargo(id string, cargo int) error {
	m.Lock()
	defer m.Unlock()
	truck, ok := m.trucks[id]
	if !ok {
		return fmt.Errorf("truck not found")
	}
	truck.Cargo = cargo
	fmt.Printf("update: %v\n", truck)
	return nil
}

type Truck struct {
	Id    string
	Cargo int
}

type Truckable interface {
	loadCargo(ctx context.Context) error
	unloadCargo(ctx context.Context) error
}

type truckManager struct {
	trucks map[string]*Truck
	sync.RWMutex
}

func NewTruckManager() truckManager {
	return truckManager{
		trucks: make(map[string]*Truck),
	}
}

type NormalTruck struct {
	Truck
}

type ElectricTruck struct {
	Truck
	Battery float64
}

func (n *NormalTruck) loadCargo(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.Cargo += 1
	time.Sleep(time.Second)
	return nil
}
func (n *NormalTruck) unloadCargo(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.Cargo = 0
	time.Sleep(time.Second)
	return nil
}

func (e *ElectricTruck) loadCargo(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.Cargo += 1
	e.Battery -= 1
	time.Sleep(time.Second)
	return nil
}
func (e *ElectricTruck) unloadCargo(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	e.Cargo = 0
	e.Battery -= 1
	time.Sleep(time.Second)
	return nil
}

func processTruck(ctx context.Context, truck Truckable) error {

	log.Printf("started processing truck: %+v\n", truck)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*500)
	defer cancel()

	err := truck.loadCargo(ctx)
	if err != nil {
		return fmt.Errorf("error loading cargo: %w", err)
	}

	err = truck.unloadCargo(ctx)
	if err != nil {
		return fmt.Errorf("error unloading cargo: %w", err)
	}

	log.Printf("finished processing truck: %+v\n", truck)

	return nil
}

func processFleet(ctx context.Context, trucks []Truckable) error {
	var wg sync.WaitGroup
	// add buffered errors channel
	errorsChan := make(chan error, len(trucks))

	for _, truck := range trucks {
		wg.Add(1)
		go func(t Truckable) {
			err := processTruck(ctx, t)
			if err != nil {
				// send err to channel
				errorsChan <- err
			}
			wg.Done()
		}(truck)
	}

	// wait all done
	wg.Wait()

	/*
	 * we could not use `defer close(errorsChan)`,
	 * reading from channel with `range`, we must explicitly close the channel to prevent deadlock.
	 */
	// close the channel
	close(errorsChan)

	var errs []error

	for err := range errorsChan {
		log.Printf("error processing truck: %+v\n", err)
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("fleet processing had %d errors", len(errs))
	}

	return nil
}

func main() {

	m := make(map[string]int)

	m["a"] = 1
	m["b"] = 2
	m["c"] = 3

	m2 := maps.Clone(m)

	log.Printf("m: %p\n", &m)
	log.Printf("m2: %p\n", &m2)

	if value, ok := m["b"]; ok {
		log.Printf("b is exist with value: %+v\n", value)
	}

	delete(m, "a")

	if _, ok := m["a"]; !ok {
		log.Println("a is not exist in map")
	}

	log.Printf("map content is %v\n", m)
	log.Printf("map2 content is %v\n", m2)

	log.Printf("m and m2 is equal = %v", maps.Equal(m, m2))

	ctx := context.Background()

	fleet := []Truckable{
		&NormalTruck{
			Truck: Truck{Id: "NT1", Cargo: 0},
		},
		&NormalTruck{
			Truck: Truck{Id: "NT2", Cargo: 0},
		},
		&ElectricTruck{
			Truck:   Truck{Id: "ET1", Cargo: 0},
			Battery: 100,
		},
		&ElectricTruck{
			Truck:   Truck{Id: "ET2", Cargo: 0},
			Battery: 100,
		},
	}

	err := processFleet(ctx, fleet)
	if err != nil {
		log.Printf("error processing fleet: %v\n", err)
		return
	}

	log.Println("all fleet processed successfully")
}

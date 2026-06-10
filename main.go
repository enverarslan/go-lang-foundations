package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type Truck interface {
	loadCargo(ctx context.Context) error
	unloadCargo(ctx context.Context) error
}

type NormalTruck struct {
	id    string
	cargo int
}

type ElectricTruck struct {
	id      string
	cargo   int
	battery float64
}

func (n *NormalTruck) loadCargo(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.cargo += 1
	time.Sleep(time.Millisecond * 250)
	return nil
}
func (n *NormalTruck) unloadCargo(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.cargo = 0
	time.Sleep(time.Millisecond * 250)
	return nil
}

func (e *ElectricTruck) loadCargo(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.cargo += 1
	e.battery -= 1
	time.Sleep(time.Millisecond * 250)
	return nil
}

func (e *ElectricTruck) unloadCargo(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	e.cargo = 0
	e.battery -= 1
	time.Sleep(time.Millisecond * 250)
	return nil
}

func processTruck(ctx context.Context, truck Truck) error {

	fmt.Printf("started processing truck: %+v\n", truck)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*250)
	defer cancel()

	err := truck.loadCargo(ctx)
	if err != nil {

		return fmt.Errorf("error loading cargo: %w", err)
	}

	err = truck.unloadCargo(ctx)
	if err != nil {
		return fmt.Errorf("error unloading cargo: %w", err)
	}

	fmt.Printf("finished processing truck: %+v\n", truck)

	return nil
}

func processFleet(ctx context.Context, trucks []Truck) error {
	var wg sync.WaitGroup

	for _, truck := range trucks {
		wg.Add(1)
		go func(t Truck) {
			err := processTruck(ctx, t)

			if err != nil {
				log.Println(err)
			}

			wg.Done()
		}(truck)
	}

	wg.Wait()

	return nil
}

func main() {

	ctx := context.Background()

	fleet := []Truck{
		&NormalTruck{id: "NT1", cargo: 0},
		&NormalTruck{id: "N2", cargo: 0},
		&ElectricTruck{id: "ET1", cargo: 0, battery: 100},
		&ElectricTruck{id: "ET2", cargo: 0, battery: 100},
	}

	err := processFleet(ctx, fleet)
	if err != nil {
		fmt.Printf("error processing fleet: %v\n", err)
		return
	}

	fmt.Println("all fleet processed successfully")
}

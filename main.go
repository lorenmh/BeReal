package main

import (
	"fmt"
	"log"
	"sort"
	"sync/atomic"
	"time"
)

// TODO:
/*
problems:
	batching
	buffering of channels
	improvement to units
	unit tests
	handling of full channels
	config structs
*/

var logger = log.Default()

const (
	GramsBeansPerOunceCoffee = 2
	GramsBeansPerOunceWater  = 0.5

	PipelineInputBufferSize  = 100
	PipelineOutputBufferSize = 100

	WorkerInputBufferSize = 10

	WorkerPoolOutputBufferSize       = 100
	WorkerPoolSchedulerSleepDuration = 10 * time.Millisecond
)

type BeanState int
type BeanTask func(*Beans) *Beans

const (
	BeanStateUnground BeanState = iota
	BeanStateGround
	BeanStateBrewed
)

type SchedulingStrategy int

func (s SchedulingStrategy) String() string {
	switch s {
	case SchedulingStrategyPushRoundRobin:
		return "push-round-robin"
	case SchedulingStrategyPushFastestAvailable:
		return "push-fastest"
	case SchedulingStrategyPull:
		return "pull"
	}
	return ""
}

const (
	// Worker is selected by round robin
	SchedulingStrategyPushRoundRobin SchedulingStrategy = iota
	// Worker is selected by fastest available
	SchedulingStrategyPushFastestAvailable
	// Worker pulls from a shared queue
	SchedulingStrategyPull
)

type IO struct {
	// Handles input and output channels
	input  chan *Beans
	output chan *Beans
}

type Pipeline struct {
	// A pipeline is a series of worker pools (stages)
	IO
}

func NewPipeline(id string, grinders []*Grinder, brewers []*Brewer, strategy SchedulingStrategy) *Pipeline {
	// Creates a new Pipeline
	pipelineIO := IO{
		input:  make(chan *Beans, PipelineInputBufferSize),
		output: make(chan *Beans, PipelineOutputBufferSize),
	}

	// the repeated code to create WorkerPools could be put into its own function but I omitted that for this assignment

	// create the grinder worker pool
	grinderTasks := make([]BeanTask, 0, len(grinders))
	grinderThroughputs := make([]int, 0, len(grinders))
	for _, grinder := range grinders {
		grinderTasks = append(grinderTasks, grinder.Grind)
		grinderThroughputs = append(grinderThroughputs, grinder.gramsPerSecond)
	}

	grinderPool := NewWorkerPool(
		fmt.Sprintf("%s:grinder", id), // pool id
		IO{
			input:  pipelineIO.input,
			output: make(chan *Beans, WorkerPoolOutputBufferSize),
		},
		grinderTasks,
		grinderThroughputs,
		strategy,
	)

	// start the grinder scheduler
	go grinderPool.Schedule()

	// create the brewer worker pool
	brewerTasks := make([]BeanTask, 0, len(brewers))
	brewerThroughputs := make([]int, 0, len(brewers))
	for _, brewer := range brewers {
		brewerTasks = append(brewerTasks, brewer.Brew)
		brewerThroughputs = append(brewerThroughputs, brewer.ouncesWaterPerSecond)
	}

	brewerPool := NewWorkerPool(
		fmt.Sprintf("%s:brewer", id), // pool id
		IO{
			input:  grinderPool.output,
			output: pipelineIO.output,
		},
		brewerTasks,
		brewerThroughputs,
		strategy,
	)

	// start the brewer scheduler
	go brewerPool.Schedule()

	return &Pipeline{
		IO: pipelineIO,
	}
}

type Worker struct {
	// A worker does work by using its `task` function. The `task` function is expected to block.
	IO
	id         string
	busy       atomic.Bool
	task       BeanTask
	throughput int // needed to determine the fastest worker
}

func NewWorker(id string, io IO, task BeanTask, throughput int) *Worker {
	// Creates a new Worker
	return &Worker{
		IO:         io,
		id:         id,
		task:       task,
		throughput: throughput,
	}
}

func (w *Worker) Work() {
	for {
		beans := <-w.input
		logger.Println(w.id, "received", beans)
		beans = w.task(beans)
		logger.Println(w.id, "processed", beans)
		w.output <- beans
		w.busy.Store(false)
	}
}

type WorkerPool struct {
	// A WorkerPool holds a collection of workers. The WorkerPool schedules the workers to do work.
	IO
	id       string
	workers  []*Worker
	strategy SchedulingStrategy
}

func NewWorkerPool(
	id string,
	io IO,
	workerTasks []BeanTask,
	workerThroughputs []int,
	strategy SchedulingStrategy,
) *WorkerPool {
	// Creates a new WorkerPool from the given worker tasks and worker throughputs

	workers := make([]*Worker, 0, len(workerTasks))
	for i, task := range workerTasks {
		var workerInput chan *Beans

		if strategy == SchedulingStrategyPull {
			// for a pull strategy, the worker should pull from the shared channel
			workerInput = io.input
		} else {
			// for a push strategy, the worker should have its own channel so that it can be scheduled
			workerInput = make(chan *Beans, WorkerInputBufferSize)
		}

		worker := NewWorker(
			fmt.Sprintf("%s[%d]", id, i), // worker id
			IO{
				input:  workerInput,
				output: io.output,
			},
			task,
			workerThroughputs[i],
		)

		workers = append(workers, worker)

		go worker.Work()
	}

	return &WorkerPool{
		IO:       io,
		id:       id,
		workers:  workers,
		strategy: strategy,
	}
}

func (wp *WorkerPool) scheduleRoundRobin() {
	// schedules workers in a round robin fashion
	cursor := 0
	for {
		beans := <-wp.input
		worker := wp.workers[cursor]
		worker.busy.Store(true)
		worker.input <- beans
		cursor = (cursor + 1) % len(wp.workers)
	}
}

func (wp *WorkerPool) fastestAvailableWorker() *Worker {
	// returns the fastest available (busy is false) worker
	// can indefinitely block if no worker is available

	for {
		availableWorkers := make([]*Worker, 0, len(wp.workers))
		for _, worker := range wp.workers {
			if !worker.busy.Load() {
				availableWorkers = append(availableWorkers, worker)
			}
		}
		if len(availableWorkers) == 0 {
			// can loop indefinitely if there are no available workers!
			time.Sleep(WorkerPoolSchedulerSleepDuration)
			continue
		}
		var fastestWorker *Worker
		var fastestThroughput int
		for _, worker := range availableWorkers {
			if worker.throughput > fastestThroughput {
				fastestWorker = worker
				fastestThroughput = worker.throughput
			}
		}
		return fastestWorker
	}
}

func (wp *WorkerPool) scheduleFastestAvailable() {
	// schedules the fastest available worker
	for {
		beans := <-wp.input
		worker := wp.fastestAvailableWorker()
		worker.busy.Store(true)
		worker.input <- beans
	}
}

func (wp *WorkerPool) Schedule() {
	switch wp.strategy {
	case SchedulingStrategyPushRoundRobin:
		wp.scheduleRoundRobin()
	case SchedulingStrategyPushFastestAvailable:
		wp.scheduleFastestAvailable()
	case SchedulingStrategyPull:
		// do nothing, the workers will pull from the shared channel
	}
}

type Metrics struct {
	// Metrics holds the performance metrics of a coffee shop
	timeTakenPerOrder []time.Duration
	totalTimeTaken    time.Duration
	ordersTaken       int
	ordersServed      int
	ozCoffeeServed    float32
}

func (m *Metrics) Print() {
	// Prints the metrics

	// sort the time taken per order to calculate the best, median, and worst durations
	sortedDurations := make([]time.Duration, len(m.timeTakenPerOrder))
	copy(sortedDurations, m.timeTakenPerOrder)
	sort.Slice(sortedDurations, func(i, j int) bool {
		return sortedDurations[i] < sortedDurations[j]
	})

	bestDuration := sortedDurations[0]
	medianDuration := sortedDurations[len(sortedDurations)/2]
	worstDuration := sortedDurations[len(sortedDurations)-1]

	fmt.Println("Metrics{")
	fmt.Println("  bestDuration:", bestDuration)
	fmt.Println("  medianDuration:", medianDuration)
	fmt.Println("  worstDuration:", worstDuration)
	fmt.Println("  totalTimeTaken:", m.totalTimeTaken)
	fmt.Println("  ordersTaken:", m.ordersTaken)
	fmt.Println("  ordersServed:", m.ordersServed)
	fmt.Println("  ozCoffeeServed:", m.ozCoffeeServed)
	fmt.Println("}")
}

type Beans struct {
	units float32
	state BeanState
}

func (b *Beans) String() string {
	var unit string
	switch b.state {
	case BeanStateUnground:
		unit = "g"
	case BeanStateGround:
		unit = "g"
	case BeanStateBrewed:
		unit = "oz"
	}
	return fmt.Sprintf("Beans{units: %.1f%s}", b.units, unit)
}

type Order struct {
	ouncesOfCoffeeWanted int

	// startTime is necessary to calculate the time taken to serve the order
	startTime time.Time
}

func (o *Order) String() string {
	return fmt.Sprintf("Order{ouncesOfCoffeeWanted: %d}", o.ouncesOfCoffeeWanted)
}

type Coffee struct {
	amountOunces float32
}

type Grinder struct {
	gramsPerSecond int
}

func (g *Grinder) Grind(beans *Beans) *Beans {
	// using time.Millisecond to get millisecond-level precision
	dur := time.Duration(1000.0*beans.units/float32(g.gramsPerSecond)) * time.Millisecond
	time.Sleep(dur)
	beans.state = BeanStateGround
	return beans
}

type Brewer struct {
	ouncesWaterPerSecond int
}

func (b *Brewer) Brew(beans *Beans) *Beans {
	waterUnits := beans.units * GramsBeansPerOunceWater
	// using time.Millisecond to get millisecond-level precision
	dur := time.Duration(1000.0*waterUnits/float32(b.ouncesWaterPerSecond)) * time.Millisecond
	time.Sleep(dur)
	beans.units = waterUnits
	beans.state = BeanStateBrewed
	return beans
}

type CoffeeShop struct {
	// A CoffeeShop takes orders and serves coffee
	id          string
	pipeline    *Pipeline
	readyCoffee *Coffee
	orders      []*Order
	metrics     *Metrics
}

func NewCoffeeShop(grinders []*Grinder, brewers []*Brewer, strategy SchedulingStrategy) *CoffeeShop {
	// Creates a new CoffeeShop
	id := fmt.Sprintf("coffee-shop-%s", strategy)
	return &CoffeeShop{
		id:          id,
		pipeline:    NewPipeline(id, grinders, brewers, strategy),
		readyCoffee: &Coffee{},
		orders:      []*Order{},
		metrics:     &Metrics{},
	}
}

func (cs *CoffeeShop) TakeOrder(order *Order) {
	// Takes an order; care should be taken to make sure this doesn't block
	cs.metrics.ordersTaken += 1

	order.startTime = time.Now()

	cs.orders = append(cs.orders, order)

	ungroundBeans := &Beans{
		units: float32(GramsBeansPerOunceCoffee * order.ouncesOfCoffeeWanted),
	}

	// send the unground beans to the pipeline
	// this could block if the pipeline's input buffer is full
	cs.pipeline.input <- ungroundBeans

	logger.Println(cs.id, "took order", order)
}

func (cs *CoffeeShop) ServeCoffee() {
	// Serve coffee in order.
	// This function will block until all of the orders are served

	for i, order := range cs.orders {
		for {
			// get processed coffee from the pipeline output
			beans := <-cs.pipeline.output

			// add to our reservoir
			cs.readyCoffee.amountOunces += beans.units

			// there's enough coffee to serve the order, break out of this loop
			if order.ouncesOfCoffeeWanted <= int(cs.readyCoffee.amountOunces) {
				break
			}
		}

		// consume the coffee from the reservoir
		cs.readyCoffee.amountOunces -= float32(order.ouncesOfCoffeeWanted)

		// collect metrics and serve
		cs.metrics.ordersServed += 1
		cs.metrics.timeTakenPerOrder = append(cs.metrics.timeTakenPerOrder, time.Now().Sub(order.startTime))
		cs.metrics.ozCoffeeServed += float32(order.ouncesOfCoffeeWanted)
		logger.Println(cs.id, "served order", i)
	}
}

func main() {
	// heterogeneous grinders
	g1 := &Grinder{gramsPerSecond: 5}
	g2 := &Grinder{gramsPerSecond: 3}
	g3 := &Grinder{gramsPerSecond: 12}

	// heterogeneous brewers
	b1 := &Brewer{ouncesWaterPerSecond: 100}
	b2 := &Brewer{ouncesWaterPerSecond: 25}

	// CoffeeShop that uses round robin scheduling
	pushRoundRobinCoffeeShop := NewCoffeeShop(
		[]*Grinder{g1, g2, g3},
		[]*Brewer{b1, b2},
		SchedulingStrategyPushRoundRobin,
	)

	// CoffeeShop that finds the fastest available worker to do the work
	pushFastestAvailableCoffeeShop := NewCoffeeShop(
		[]*Grinder{g1, g2, g3},
		[]*Brewer{b1, b2},
		SchedulingStrategyPushFastestAvailable,
	)

	// CoffeeShop where workers pull from a queue
	pullCoffeeShop := NewCoffeeShop(
		[]*Grinder{g1, g2, g3},
		[]*Brewer{b1, b2},
		SchedulingStrategyPull,
	)

	coffeeShops := []*CoffeeShop{
		pushRoundRobinCoffeeShop,
		pushFastestAvailableCoffeeShop,
		pullCoffeeShop,
	}

	// run all of the simulations
	numCustomers := 10
	for _, coffeeShop := range coffeeShops {
		startTime := time.Now()
		for i := 0; i < numCustomers; i++ {
			order := &Order{
				ouncesOfCoffeeWanted: 12,
			}
			coffeeShop.TakeOrder(order)
		}
		coffeeShop.ServeCoffee()
		coffeeShop.metrics.totalTimeTaken = time.Now().Sub(startTime)
	}

	// print metrics
	for _, coffeeShop := range coffeeShops {
		fmt.Println("==================================")
		fmt.Printf("%s results\n", coffeeShop.id)
		fmt.Println("==================================")
		coffeeShop.metrics.Print()
	}
}

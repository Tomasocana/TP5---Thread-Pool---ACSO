/**
 * File: thread-pool.cc
 * --------------------
 * Presents the implementation of the ThreadPool class.
 */

#include "thread-pool.h"
using namespace std;

ThreadPool::ThreadPool(size_t numThreads) : wts(numThreads), done(false), tasksInProgress(0) {
    /// Inicializo el semaforo de trabajadores libres
    for (size_t i = 0; i < numThreads; i++) {
        freeWorker.signal();
    }
    
    // Inicializo el dispatcher
    dt = thread([this] { dispatcher(); }); 

    // Inicializo la pool de workers
    for (size_t i = 0; i < numThreads; i++) {
        wts[i].free = true;
        wts[i].workerId = i + 1;
        wts[i].ts = thread([this, i] { worker(i); });
    }

}

void ThreadPool::dispatcher() {
    // Mientras la pool no sea destruida
    while (!done) {
        // Espera a que el scheduler agregue tareas a la cola
        tasksAvailable.wait();

        // Espera a que haya un worker libre
        freeWorker.wait();

        // Bloquea el acceso a la cola
        lock_guard<mutex> lg(queueLock);

        // Y comienza a iterar entre los workers hasta encontrar uno disponible para 
        // posteriormente asignarle la tarea
        for (size_t i = 0; i < wts.size(); i++) {
            if (wts[i].free && !taskQueue.empty()) {
                wts[i].thunk = taskQueue.back();
                taskQueue.pop_back();
                wts[i].free = false;
                wts[i].work.signal();
            }
        }
    }
}

void ThreadPool::worker(int id) {
    while(!done){
        // Espera a que el dispatcher le asigne una tarea
        wts[id].work.wait();

        /// Si la pool está siendo destruida, sale del loop
        if (done) break;

        // Ejecuta la tarea asignada
        wts[id].thunk();

        // Si terminó de trabajar:
        wts[id].free = true;  // Marca que está libre
        freeWorker.signal();  // Y le avisa al dispatcher

        // Disminuye el contador de tareas en progreso
        int remaining = --tasksInProgress;
        // Y se fija si era la última tarea para despertar al wait
        if (remaining == 0) tasksDone.signal();
    }
}

void ThreadPool::schedule(const function<void(void)>& thunk) {
    // Aumenta las tareas en progreso
    tasksInProgress++;

    // Bloquea el acceso a la cola
    lock_guard<mutex> lg(queueLock);

    // Pushea la tarea a la cola
    taskQueue.push_front(thunk);

    // Señala al dispatcher que hay tareas disponibles
    tasksAvailable.signal();
}

void ThreadPool::wait() {
    if (tasksInProgress > 0) {
        tasksDone.wait();
    }
}

ThreadPool::~ThreadPool() {
    // Espera a que todas las tareas terminen
    wait();

    done = true;

    // Despierta al dispatcher y a los workers para que terminen
    tasksAvailable.signal();
    freeWorker.signal();
    for (size_t i = 0; i < wts.size(); i++) {
        wts[i].work.signal();
    }

    /// Espero a que el dispatcher termine
    if (dt.joinable()) {
        dt.join();
    }   

    // Uno los hilos
    for (size_t i = 0; i < wts.size(); i++) {
        if (wts[i].ts.joinable()) {
            wts[i].ts.join();
        }
    }
}
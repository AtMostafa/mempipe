# mempipe

This package combines the simplicity of using Python's `multiprocessing.Pipe()` with the speed of `multiprocessing.shared_memory.SharedMemory`.
Meant for a multiprocessing data analysis pipeline where each process analyses the data and passes it to the next process for the next step.

The passed data can only be a numpy array.

Originally designed for a real-time Brain-Computer-Interface application.

## How to use

Import the `MemPipe` class.
Make an instance object.
Call `YourMemPipeInstance.Pipe(ex_array)` instead of the `multiprocessing.Pipe(duplex=False)` in your applicaiton.
The `ex_array` is a numpy array that is used to infer the `dtype` and the size of the shared memory.
It must be the biggest array size that you will intend to pass between the processes.
If you `.send()` *larger* numpy arrays, they will be truncated to `ex_array.shape`.
If you `.send()` *smaller* numpy arrays, they will be handled just fine.

The pipe objects support `poll()`, `recv()`, and `send()`.
Before calling `recv()`, you must call `poll()` and make sure it returns `True` (as you would normally).

All else is handled internally.  
Currently, [./tests/test2.py](./tests/test2.py) shows an example using the latest API.

## Simulation

> This was done using an older version of the package

My simple simulation (found here: [./tests/test3.py](./tests/test3.py)) shows that passing a numpy array of $12000 \times 4000$ is much faster this way, compared to regular pipes.

![a figure](./docs/Figure_1.png)  
20 repeatitions on the x-axis, and the execution time on the y-axis.

# pymasp
Python Modular Automated Scan Programm for pump-probe spectroscopy

pymasp (working title) is a software to controll data acquisition of a scientific experiment such as a pump-probe spectroscopy experiment.

It is designed to be modular on several levels, so that quick adaptation to changing needs of a scientific environment can be done without cornering the programm into a single use-case.

## Status
pymasp is currently not in a working status, but rather in its design phase. Comments and suggestions are welcome. For a extensive list of things missing see Roadmap.

## pymaspd
The main component of pymasp is pymaspd, the pymasp daemon. It automatically works a list of experiments, consisting of parameters and jobs (see below), records an automatic lab journal and saves and delivers data via its pymasp-journal component and gives access to lab equipment for a unified calibration/aligment process pre- and control overview during experiments.
pymaspd doesn't provide a user interface, but an API via zeromq to communicate with user interfaces such as pymasp-web.

### Scientific Experiments
pymasp is designed to be able to perform a wide variety of (spectropscopic) experiments. It is therefore written to be easily extensible for a variety of lab equipment.
In a (spectroscopic) experiment, data from one or more detectors is recorded as a function of one or more experimental conditions (parameters). A simple experiment might be the subsequential recording of data from the same detector under the same conditions (repeat), recording of data at predefined parameter values (list) or recording of data at a range of parameter values (iterator). Experiments using a range of parameter values might be recorded in a looping, a bouncing or random varietion of values, or might be a complex nesting of all cases.

To accompany the demands of this abstract definition of an experiment, pymasp works a queue of experiments, where experiments are a tree structure consisting of:

Jobs:

- Encapsulating elements (Iterators, Lists, Loops, ...)
- Detectors (Spectrometers, Cameras, Oscilloscopes, ...)

Parameters:

- Motorized Linear Stages, Mirrors, Irises, Drivers ...

For example one experiment might consist of an iterator, that controls a linear stage over a 100 positions between -1 and +1 mm and for every step records one image from a camera.

Experiments can be duplicated, saved and loaded for reuse.

### Add your own detectors, parameters or jobs

### API documentation

### pymasp-journal: Database backend


## pymasp-web
pymasp-web is a Flask web-application that provides a Web-GUI via http. It will feature a customizable on-line evaluation of data via d3


## Roadmap
POC:

- Complete this README
- Complete preliminary API
- Write a test suite for the API
- Bootstrap the pymasp-journal
- Bootstrap pymasp-web
 
Alpha:

- Complete pymasp-journal
- Add some real world devices
- Provide documentation on how to add new devices
 
Beta:

- Reach feature level of old "Scan Programm"
- Complete pymasp-web
- Bootstrap pymasp-qt

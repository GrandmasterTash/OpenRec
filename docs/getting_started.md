# Getting Started

To try some of the example controls in docker you must use the following (assumes you have a Linux environment, docker and docker-compose installed): -

```bash
cd docker
./openrec.sh
```

This will build OpenRec (the first time will take a LONG time), then run Steward with a few of the example controls running.

You can now load some initial data into the controls:

```bash
./load_data1.sh
```

If you're looking at Steward in the console mode, you'll see a flash as the data is put into the inbox of each respective control and then, typically becomes unmatched data available in the outbox folders.

Now load some more data:

```bash
./load_data2.sh
```

This should complete the match for most of the example controls.

In addition, you can generate random data for the *performance* control with the script `./random_data.sh`.

Now try visiting [http://localhost:3000](http://localhost:3000) to see the example **Grafana** dashboard (credentials admin/admin).


## Manually Invoking Components
You can run steward, jetwash and celerity as stand-alone command-line binaries rather than as part of a docker setup. Simply provide the help argument to see more details. For example (assuming the binaries are on the PATH): -

```bash
jetwash --help
```

Similarly

```bash
celerity --help
```

## What Next?

If you haven't already read the [concepts](concepts.md) section you should do that now. Combine that with the 'kitchen sink' example charter as a reference and you should now be able to craft your own charters for your own data!
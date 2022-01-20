# Examples

The [examples](/examples) folder contains a series of charter examples which cover the various features of OpenRec. You should read through them, as-in turn, they progressively introduce more and more configuration concepts and matching scenarios.

The last example [The-Kitchen-Sink](/examples/13-The-Kitchen-Sink.yaml) contains a full reference of all charter configuration.

## Docker Playground

When familiar with the examples you should jump on over to the [Getting Started](getting_started.md) section. The docker playground will get Steward up and running to automate the initiation of match jobs.

## Running Manually

If you want to run them manually though, you can run jetwash to pre-process data and then celerity to match the data.

Start by compiling the workspace - from the root project folder: -

```bash
cargo build --release
```

Now you'll have jetwash and celerity in the /target/release folder.

Run them both with --help to see the arguments they accept - but essentially they both take a path to a charter file and a path to a base data folder to use. For example: -


```bash
mkdir -p ~/tmp/control_a/inbox
cp ./examples/data/01-invoices.csv ~/tmp/control_a/inbox/
cp ./examples/data/01-payments.csv ~/tmp/control_a/inbox/
./target/release/jetwash ./examples/01-Basic-Match.yaml ~/tmp/control_a
./target/release/celerity ./examples/01-Basic-Match.yaml ~/tmp/control_a
```
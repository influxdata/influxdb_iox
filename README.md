# InfluxDB IOx

TODO: Blog post summary here

## Quick Start

To compile and run InfluxDB IOx from source, you'll need a Rust compiler and a `flatc` FlatBuffers
compiler.

### Cloning the Repository

Using `git`, check out the code by cloning this repository. If you use the `git` command line, this
looks like:

```
$ git clone git@github.com:influxdata/influxdb_iox.git
```

Then change into the directory containing the code:

```
$ cd influxdb_iox
```

The rest of the instructions assume you are in this directory.

### Installing Rust

The easiest way to install Rust is by using [`rustup`], a Rust version manager.
Follow the instructions on the `rustup` site for your operating system.

[`rustup`]: https://rustup.rs/

By default, `rustup` will install the latest stable verison of Rust. InfluxDB IOx is currently
using a nightly version of Rust to get performance benefits from the unstable `simd` feature. The
exact nightly version is specified in the `rust-toolchain` file. When you're in the directory
containing this repository's code, `rustup` will look in the `rust-toolchain` file and
automatically install and use the correct Rust version for you. Test this out with:

```
$ rustc --version
```

and you should see a nightly version of Rust!

### Installing `flatc`

InfluxDB IOx uses the [FlatBuffer] serialization format for its write-ahead log. The [`flatc`
compiler] reads the schema in `generated_types/wal.fbs` and generates the corresponding Rust code.

Install `flatc` >= 1.12.0 with one of these methods as appropriate to your operating system:

- Using a [Windows binary release]
- Using the [`flatbuffers` package for conda]
- Using the [`flatbuffers` package for Arch Linux]
- Using the [`flatbuffers` package for Homebrew]

Once you have installed the packages, you should be able to run:

```
$ flatc --version
```

and see the version displayed.

You won't have to run `flatc` directly; once it's available, Rust's Cargo build tool manages the
compilation process by calling `flatc` for you.

[FlatBuffer]: https://google.github.io/flatbuffers/
[`flatc` compiler]: https://google.github.io/flatbuffers/flatbuffers_guide_using_schema_compiler.html
[Windows binary release]: https://github.com/google/flatbuffers/releases
[`flatbuffers` package for conda]: https://anaconda.org/conda-forge/flatbuffers
[`flatbuffers` package for Arch Linux]: https://www.archlinux.org/packages/community/x86_64/flatbuffers/
[`flatbuffers` package for Homebrew]: https://github.com/Homebrew/homebrew-core/blob/HEAD/Formula/flatbuffers.rb

### Specifying Configuration

**OPTIONAL:** There are a number of configuration variables you can choose to customize by
specifying values for environment variables in a `.env` file. To get an example file to start from,
run:

```
cp docs/env.example .env
```

then edit the newly-created `.env` file.

For development purposes, the most relevant environment variables are the `INFLUXDB_IOX_DB_DIR` and
`TEST_INFLUXDB_IOX_DB_DIR` variables that configure where files are stored on disk. The default
values are shown in the comments in the example file; to change them, uncomment the relevant lines
and change the values to the directories in which you'd like to store the files instead:

```
INFLUXDB_IOX_DB_DIR=/some/place/else
TEST_INFLUXDB_IOX_DB_DIR=/another/place
```

### Compiling and Starting the Server

InfluxDB IOx is built using Cargo, Rust's package manager and build tool.

To compile for development, run:

```
$ cargo +nightly build
```

which will create a binary in `target/debug` that you can run with:

```
$ ./target/debug/influxdb_iox
```

You can compile and run with one command by using:

```
$ cargo +nightly run
```

When compiling for performance testing, build in release mode by using:

```
$ cargo +nightly build --release
```

which will create the corresponding binary in `target/release`:

```
$ ./target/release/influxdb_iox
```

Similarly, you can do this in one step with:

```
$ cargo +nightly run --release
```

The server will, by default, start an HTTP API server on port `8080` and a gRPC server on port
`8082`.

### Writing and Reading Data

Data can be stored in InfluxDB IOx by sending it in [line protocol] format to the `/api/v2/write`
endpoint. Data is stored by organization and bucket names. Here's an example using [`curl`] with
the organization name `company` and the bucket name `sensors` that will send the data in the
`tests/fixtures/lineproto/metrics.lp` file in this repository, assuming that you're running the
server on the default port:

```
curl -v "http://127.0.0.1:8080/api/v2/write?org=company&bucket=sensors" --data-binary @tests/fixtures/lineproto/metrics.lp
```

[line protocol]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
[`curl`]: https://curl.se/

To query stored data, use the `/api/v2/read` endpoint with a SQL query. This example will return
all data in the `company` organization's `sensors` bucket for the `processes` measurement:

```
$ curl -v -G -d 'org=company' -d 'bucket=sensors' --data-urlencode 'sql_query=select * from processes' "http://127.0.0.1:8080/api/v2/read"
```

## Contributing

If you want to contribute to InfluxDB IOx you will need to sign InfluxData's CLA, which can be
found with more information [on our website].

[on our website]: https://www.influxdata.com/legal/cla/

InfluxDB IOx is written mostly in idiomatic Rust -- please see the [Style Guide] for more details.

[Style Guide]: docs/style_guide.md

### Running Tests

The `cargo` build tool runs tests as well. Run:

```
$ cargo +nightly test --workspace
```

### Running `rustfmt` and `clippy`

CI will check the code formatting with [`rustfmt`] and Rust best practices with [`clippy`].

To automatically format your code according to `rustfmt` style, first make sure `rustfmt` is installed using `rustup`:

```
$ rustup component add rustfmt
```

Then, whenever you make a change and want to reformat, run:

```
$ cargo fmt --all
```

Similarly with `clippy`, install with:

```
$ rustup component add clippy
```

And run with:

```
$ cargo +nightly clippy --all-targets --workspace
```

[`rustfmt`]: https://github.com/rust-lang/rustfmt
[`clippy`]: https://github.com/rust-lang/rust-clippy

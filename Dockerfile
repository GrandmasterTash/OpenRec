#
# To build run this from the project root
#   docker build -t openrec .
#
# To run from the docker folder
#   docker run -i -t -v $(pwd)/openrec/etc:/etc/openrec/ -v $(pwd)/openrec/data:/data openrec:latest /steward
#
#
# cargo chef will cache compiled 3rd party crates making incremental builds much quicker.
FROM rust:1.57.0 AS chef
ENV PKG_CONFIG_ALLOW_CROSS=1
RUN cargo install cargo-chef
RUN rustup component add rustfmt
WORKDIR openrec

# Build a recipe for the chef!
FROM chef AS planner
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./celerity/src ./celerity/src
COPY ./celerity/Cargo.toml ./celerity/Cargo.toml
COPY ./core/src ./core/src
COPY ./core/Cargo.toml ./core/Cargo.toml
COPY ./generator/src ./generator/src
COPY ./generator/Cargo.toml ./generator/Cargo.toml
COPY ./integration-tests/Cargo.toml ./integration-tests/Cargo.toml
COPY ./integration-tests/tests ./integration-tests/tests
COPY ./jetwash/src ./jetwash/src
COPY ./jetwash/Cargo.toml ./jetwash/Cargo.toml
COPY ./steward/src ./steward/src
COPY ./steward/Cargo.toml ./steward/Cargo.toml
RUN cargo chef prepare  --recipe-path recipe.json

# Build the app from the recipe.
FROM chef AS builder
COPY --from=planner /openrec/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./celerity/src ./celerity/src
COPY ./celerity/Cargo.toml ./celerity/Cargo.toml
COPY ./core/src ./core/src
COPY ./core/Cargo.toml ./core/Cargo.toml
COPY ./generator/src ./generator/src
COPY ./generator/Cargo.toml ./generator/Cargo.toml
COPY ./integration-tests/Cargo.toml ./integration-tests/Cargo.toml
COPY ./integration-tests/tests ./integration-tests/tests
COPY ./jetwash/src ./jetwash/src
COPY ./jetwash/Cargo.toml ./jetwash/Cargo.toml
COPY ./steward/src ./steward/src
COPY ./steward/Cargo.toml ./steward/Cargo.toml
RUN cargo build --release

# The final image.
FROM gcr.io/distroless/cc AS runtime
WORKDIR /
COPY --from=builder /openrec/target/release/celerity .
COPY --from=builder /openrec/target/release/generator .
COPY --from=builder /openrec/target/release/jetwash .
COPY --from=builder /openrec/target/release/steward .
CMD ["/steward"]

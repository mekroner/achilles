# Achilles

## What is Achilles
Achilles is a test framework for the NebulaStream project.
It uses metamorphic testing to verify the correctness of queries.

## Dependencies
Achilles requires two rust dependencies that have to be installed manually.
The path to both dependencies must be set in the `Cargo.toml` file.

- **nes-types**: The [nes-types](https://github.com/mekroner/nes-types) crate defines the types used by nebula stream as a rust enum
- **nes-rust-client**: The [nes-rust-client](https://github.com/mekroner/nes-rust-client) crate provides a client-side API for the NebulaStream runtime.
  In addition, it defines the `Query` and `Expression` types. It serializes these types into [Protocol Buffer](https://protobuf.dev/) and handles communication with the NebulaStream runtime.

 To test NebulaStream, the user must provide Achilles with the path to the executable of the NES worker and NES coordinator.
 This is done via Achilles' configuration.

  

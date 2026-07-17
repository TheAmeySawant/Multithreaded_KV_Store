fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("Failed to find bundled protoc");


    unsafe {
        //Works on windows
        //But on other OS, it probably won't work because this program is Multithreaded!
        std::env::set_var("PROTOC", protoc);
    }

    prost_build::compile_protos(
        &["proto/kvstore.proto"],
        &["proto"],
    )
    .expect("Failed to compile .proto files");
}
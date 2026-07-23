from pathlib import Path


def test_runtime_image_packages_dedicated_v4_graph_compiler() -> None:
    dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text()

    assert "cargo build --release --bins" in dockerfile
    assert (
        "ENV HLTHPRT_PTG2_PROVIDER_GRAPH_V4_BIN="
        "/opt/support/ptg2_scanner/target/release/ptg2_provider_graph_v4"
    ) in dockerfile
    assert dockerfile.count(
        "/build/support/ptg2_scanner/target/release/ptg2_provider_graph_v4"
    ) == 1
    assert dockerfile.count(
        "/opt/support/ptg2_scanner/target/release/ptg2_provider_graph_v4"
    ) == 2

import pandas as pd
import tempfile
from tfsage.embedding import run_seurat_integration
from snakemake.script import snakemake


def embeddings(inputs, output_dir, params):
    # Format metadata for integration
    df = pd.read_parquet(inputs.metadata)
    df.set_index("File accession", inplace=True)
    df.index.name = None

    with tempfile.NamedTemporaryFile() as tmp_file:
        df.to_parquet(tmp_file.name)
        run_seurat_integration(
            rp_matrix=inputs.rp_matrix,
            metadata=tmp_file.name,
            output_dir=output_dir,
            align_key=params.align_key,
            methods=params.methods,
        )


embeddings(snakemake.input, snakemake.output[0], snakemake.params)

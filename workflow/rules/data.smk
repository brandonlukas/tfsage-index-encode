import os
import dask.dataframe as dd


def collect_experiments(wildcards):
    path = checkpoints.format_index.get().output[0]
    experiments = dd.read_parquet(path)["File accession"].compute()
    template = rules.download.output[0]
    return collect(template, experiment=experiments)


rule features_tss:
    input:
        collect_experiments,
    output:
        os.path.join(config["data_dir"], "rp_matrix/tss.h5"),
    threads: workflow.cores
    resources:
        mem_mb=96000,
    script:
        "../scripts/data/features_tss.py"


rule features_gene:
    input:
        rules.features_tss.output[0],
    output:
        os.path.join(config["data_dir"], "rp_matrix/gene.h5"),
    resources:
        mem_mb=96000,
    script:
        "../scripts/data/features_gene.py"


rule embeddings:
    input:
        rp_matrix=os.path.join(config["data_dir"], "rp_matrix/{features}.h5"),
        metadata=rules.format_index.output[0],
    output:
        directory(os.path.join(config["data_dir"], "embeddings/{features}")),
    params:
        align_key="Assay",
        methods=config["embedding_methods"],
    script:
        "../scripts/data/embeddings.py"

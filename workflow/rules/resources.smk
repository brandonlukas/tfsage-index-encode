checkpoint format_index:
    input:
        config["resources"]["index"],
    output:
        config["resources"]["index_parquet"],
    script:
        "../scripts/resources/format_index.py"

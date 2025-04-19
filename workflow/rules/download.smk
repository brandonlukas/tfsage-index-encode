import os
from tfsage.download import download_encode


rule download:
    output:
        os.path.join(config["downloads_dir"], "{experiment}.bed"),
    retries: 5
    run:
        download_encode(wildcards.experiment, output[0])

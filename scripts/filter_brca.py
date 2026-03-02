# [AI-Agent]: BRCA1/2 Filtering Pipeline.
# This script uses bcftools for high-performance gene-specific filtering from a ClinVar VCF.
# Following the project's minimalist pipeline-style architecture.

import os
import sys
import subprocess
from typing import Final, Self

# [AI-Agent]: Pipeline Configuration.
BRCA1_REGION: Final[str] = "chr17:43044295-43125483" # [AI-Agent]: Genomic coordinates for GRCh38.
BRCA2_REGION: Final[str] = "chr13:32315086-32400266" # [AI-Agent]: Genomic coordinates for GRCh38.
LOCAL_RAW_DIR: Final[str] = "data/raw/clinvar"
INPUT_VCF: Final[str] = os.path.join(LOCAL_RAW_DIR, "clinvar_raw.vcf.gz")
OUTPUT_VCF: Final[str] = os.path.join(LOCAL_RAW_DIR, "clinvar_brca_subset.vcf.gz")

class BRCAFilterPipeline:
    """
    [AI-Agent]: Orchestrates the gene-specific filtering pipeline for ClinVar.
    Goal: Isolate BRCA1 and BRCA2 variants from the bulk ClinVar dataset.
    """

    def __init__(self) -> None:
        self.filter_ok: bool = False

    def check_inputs(self) -> Self:
        """
        [AI-Agent]: Stage 1 - Input Validation.
        Effect: Confirms that the raw ClinVar VCF exists before processing.
        """
        if not os.path.exists(INPUT_VCF):
            print(f"❌ [Stage 1 Effect]: Missing input VCF: {INPUT_VCF}")
            sys.exit(1)
        return self

    def filter_vcf(self) -> Self:
        """
        [AI-Agent]: Stage 2 - High-Performance Filtering with bcftools.
        Effect: Extracts coordinates for BRCA1 and BRCA2 using tabix-indexed querying.
        """
        print(f"--- [BRCA Stage 2]: Filtering ClinVar for {BRCA1_REGION} and {BRCA2_REGION} ---")
        regions = f"{BRCA1_REGION},{BRCA2_REGION}"
        
        # [AI-Agent]: Command chain to filter, index, and compress in one pipeline step.
        command = [
            "bcftools", "view",
            "--regions", regions,
            "--output-type", "z",
            "--output", OUTPUT_VCF,
            INPUT_VCF
        ]
        
        try:
            subprocess.run(command, check=True)
            print(f"✅ [Stage 2 Effect]: Filtered subset saved to {OUTPUT_VCF}")
            self.filter_ok = True
        except subprocess.CalledProcessError as e:
            print(f"❌ [Stage 2 Effect]: bcftools filter failed. Error: {e}")
            sys.exit(1)
        return self

    def index_vcf(self) -> Self:
        """
        [AI-Agent]: Stage 3 - VCF Indexing.
        Effect: Creates a tabix (.tbi) index for the filtered subset to enable downstream queries.
        """
        if not self.filter_ok:
            return self

        print("--- [BRCA Stage 3]: Indexing the filtered VCF ---")
        try:
            subprocess.run(["bcftools", "index", "--tbi", OUTPUT_VCF], check=True)
            print("✅ [Stage 3 Effect]: Tabix index created.")
        except subprocess.CalledProcessError as e:
            print(f"❌ [Stage 3 Effect]: bcftools index failed. Error: {e}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        """
        [AI-Agent]: Final Stage - Status Reporting.
        Effect: Confirms the completion of the BRCA filtering pipeline.
        """
        match (self.filter_ok):
            case True:
                print("
🎉 [Final Effect]: BRCA Filtering Pipeline completed successfully!")
            case _:
                print("
⚠️  [Final Effect]: BRCA Filtering Pipeline failed.")
                sys.exit(1)

if __name__ == "__main__":
    # [AI-Agent]: Start the BRCA-specific filter sequence.
    BRCAFilterPipeline() 
        .check_inputs() 
        .filter_vcf() 
        .index_vcf() 
        .finalize()

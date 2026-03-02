# [AI-Agent: Gemini 2.0 Flash]: This script filters the ClinVar VCF for BRCA1/2 variants.
# Each class method is a stage in the variant selection pipeline.

import os
import sys
import subprocess
from typing import Final, Self

# [AI-Agent: Gemini 2.0 Flash]: Genomic coordinates for the target universe (GRCh38).
BRCA1_REGION: Final[str] = "chr17:43044295-43125483"
BRCA2_REGION: Final[str] = "chr13:32315086-32400266"
LOCAL_RAW_DIR: Final[str] = "data/raw/clinvar"
INPUT_VCF: Final[str] = os.path.join(LOCAL_RAW_DIR, "clinvar_raw.vcf.gz")
OUTPUT_VCF: Final[str] = os.path.join(LOCAL_RAW_DIR, "clinvar_brca_subset.vcf.gz")

class BRCAFilterPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the filtering and indexing pipeline for BRCA1/2 variants.
    Goal: Isolate targeted genomic regions from a bulk ClinVar dataset.
    """

    def __init__(self) -> None:
        self.filter_ok: bool = False

    def check_inputs(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - Input Validation.
        Effect: Confirms the presence of the source VCF before starting filtering.
        """
        if not os.path.exists(INPUT_VCF):
            print(f"❌ [Stage 1 Effect]: Missing input VCF: {INPUT_VCF}")
            sys.exit(1)
        return self

    def filter_vcf(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 2 - High-Performance Filtering with bcftools.
        Effect: Extracts coordinates for BRCA1 and BRCA2 using tabix-indexed querying.
        """
        print(f"--- [BRCA Stage 2]: Filtering ClinVar for {BRCA1_REGION} and {BRCA2_REGION} ---")
        regions = f"{BRCA1_REGION},{BRCA2_REGION}"
        
        # [AI-Agent: Gemini 2.0 Flash]: Sequential bcftools command chain for filtering and compression.
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
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 3 - VCF Indexing.
        Effect: Creates a tabix (.tbi) index to enable high-speed downstream querying.
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
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Status Reporting.
        Effect: Confirms the successful conclusion of the filtering pipeline.
        """
        # [AI-Agent: Gemini 2.0 Flash]: Pattern matching for terminal flow control.
        match (self.filter_ok):
            case True:
                print("\n🎉 [Final Effect]: BRCA Filtering Pipeline completed successfully!")
            case _:
                print("\n⚠️  [Final Effect]: BRCA Filtering Pipeline failed.")
                sys.exit(1)

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Initiate the filtering sequence.
    BRCAFilterPipeline() \
        .check_inputs() \
        .filter_vcf() \
        .index_vcf() \
        .finalize()
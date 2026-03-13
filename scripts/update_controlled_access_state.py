"""Build a frozen controlled-access acquisition guide for the supervisor UI.

The payload stays static inside the UI bundle so the supervisor can review
which high-value Arab datasets are still restricted, why they matter, and how
access works without triggering any live analytical jobs.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Final

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
UI_FILE: Final[Path] = ROOT / "ui" / "controlled_access.json"


# [AI-Agent: Codex]: Guide layer 1 captures the platform-level access workflows
# exactly as they are documented by the official providers.
PROCESS_GUIDES: Final[tuple[dict[str, object], ...]] = (
    {
        "key": "ega",
        "title": "EGA controlled-access workflow",
        "applies_to": "Emirati, Moroccan, Tunisian, and Egyptian controlled datasets archived at EGA",
        "why_it_exists": "EGA does not allow bulk download until the Data Access Committee (DAC) approves a named research project.",
        "official_links": [
            {
                "label": "EGA how to request data",
                "url": "https://ega-archive.org/access/request-data/how-to-request-data/",
            },
            {
                "label": "EGA register",
                "url": "https://ega-archive.org/register/",
            },
            {
                "label": "EGA datasets catalogue",
                "url": "https://ega-archive.org/datasets/",
            },
        ],
        "steps": [
            "Create an EGA account and validate it through the email link sent by EGA.",
            "Log in, open the study or dataset accession, and click Request access.",
            "Submit the project summary and intended use directly on the EGA request form.",
            "Email the dataset's DAC contact after filing the request because EGA explicitly recommends direct DAC contact.",
            "Wait for DAC review; EGA states to allow up to four weeks and to send weekly reminders if needed.",
            "After approval, use the granted download credentials to retrieve the authorized files.",
        ],
        "source_note": "Steps verified from the official EGA request-data guide reviewed on 2026-03-13.",
    },
    {
        "key": "qphi",
        "title": "QPHI research-portal workflow",
        "applies_to": "Qatar Precision Health Institute and Qatar Genome Program releases",
        "why_it_exists": "QPHI provides data and biosamples only to approved researchers through its formal portal, application, and agreement flow.",
        "official_links": [
            {
                "label": "QPHI how to apply",
                "url": "https://www.qphi.org.qa/research/how-to-apply",
            },
            {
                "label": "QPHI research portal",
                "url": "https://researchportal.qphi.org.qa/login",
            },
            {
                "label": "QPHI genomic data",
                "url": "https://www.qphi.org.qa/genomicdata",
            },
        ],
        "steps": [
            "Create a researcher profile in the QPHI Research Portal.",
            "Use the pre-application query form if you need clarification on sample availability or data type.",
            "Submit the Research Access Application Form through the portal.",
            "Complete any required IRB review and institutional paperwork described by QPHI.",
            "If remote access is needed, complete the Remote Data Access Agreement and the Remote Access Request Form listed on the QPHI how-to-apply page.",
            "Communicate with the Research Access Office at qphi-ro@qf.org.qa for research questions.",
        ],
        "source_note": "Steps verified from the official QPHI how-to-apply page reviewed on 2026-03-13.",
    },
)


# [AI-Agent: Codex]: Guide layer 2 is source-specific. Each record answers
# four questions only: what the dataset is, why it helps this project, what the
# official release evidence is, and how access works.
CONTROLLED_ACCESS_SOURCES: Final[tuple[dict[str, object], ...]] = (
    {
        "key": "emirati_population_variome",
        "display_name": "Emirati Population Variome",
        "priority": "priority_1",
        "country_or_region": "United Arab Emirates",
        "access_model": "controlled_access",
        "process_guide": "ega",
        "data_scope": "GRCh38 whole-genome population dataset; EGA metadata lists 43,608 participants and 421,605,069 variants with chromosome-split VCF files.",
        "why_we_need_it": "This is the largest Arab population-frequency source identified so far. It is the strongest candidate to replace small Emirati workbook-style frequency tables in the main evidence layer.",
        "official_release_evidence": "EGA study metadata reviewed 2026-03-13 show dataset publication on 2025-07-18 and chromosome-split VCF delivery.",
        "build_or_coordinate_note": "GRCh38 is stated in the public EGA metadata.",
        "official_links": [
            {
                "label": "EGA study EGAS50000001071",
                "url": "https://ega-archive.org/studies/EGAS50000001071/",
            },
            {
                "label": "EGA dataset EGAD50000001558",
                "url": "https://ega-archive.org/datasets/EGAD50000001558/",
            },
            {
                "label": "EGA DAC EGAC00001001544",
                "url": "https://ega-archive.org/dacs/EGAC00001001544/",
            },
            {
                "label": "EGA request guide",
                "url": "https://ega-archive.org/access/request-data/how-to-request-data/",
            },
        ],
        "access_steps": [
            "Register in EGA, validate the account, then open study EGAS50000001071 or dataset EGAD50000001558.",
            "Submit Request access from the dataset page, naming the BRCA population-frequency use case and the intended analysis outputs.",
            "Contact the listed DAC after filing the request because EGA recommends direct DAC follow-up.",
            "Once access is granted, download the chromosome-level VCF files and freeze the raw bytes to GCS before any parsing.",
        ],
        "practical_decision": "Highest-value controlled dataset to pursue after SHGP because it is large, Arab, genomic-coordinate ready, and newer than the current AVDB workbook.",
    },
    {
        "key": "qphi_qatari_25k",
        "display_name": "QPHI-Qatari 25k release",
        "priority": "priority_1",
        "country_or_region": "Qatar",
        "access_model": "controlled_access",
        "process_guide": "qphi",
        "data_scope": "QPHI genomic release page lists Version 1 with 24,838 healthy Qatar BioBank participants, cohort-level VCF plus additional cohort and participant genomic files.",
        "why_we_need_it": "A large Qatari allele-frequency resource would materially strengthen Gulf representation and reduce over-reliance on GME subgroup summaries.",
        "official_release_evidence": "QPHI public genomic-data page reviewed 2026-03-13 lists Version 1 (25k release). The public page does not expose a separate release date field, so only the release label is asserted here.",
        "build_or_coordinate_note": "The public release page confirms WGS cohort files and VCF delivery, but the reviewed public page did not explicitly expose the genome build string.",
        "official_links": [
            {
                "label": "QPHI how to apply",
                "url": "https://www.qphi.org.qa/research/how-to-apply",
            },
            {
                "label": "QPHI research portal",
                "url": "https://researchportal.qphi.org.qa/login",
            },
            {
                "label": "QPHI genomic data",
                "url": "https://www.qphi.org.qa/genomicdata",
            },
            {
                "label": "QPHI data catalog",
                "url": "https://www.qphi.org.qa/DataCatalog",
            },
        ],
        "access_steps": [
            "Create a research-portal profile and review the genomic-data release pages to define the exact data request.",
            "Submit the Research Access Application Form through the portal and include the BRCA population-frequency objective.",
            "If needed, use the pre-application query route for availability questions and the QPHI Research Access Office for follow-up.",
            "Complete the remote-access and agreement forms if QPHI approves access via a managed remote environment rather than direct file transfer.",
        ],
        "practical_decision": "High-value Gulf dataset, but operationally slower than EGA-style access because the portal, IRB, and agreement flow are provider-specific.",
    },
    {
        "key": "moroccan_genome_project",
        "display_name": "Moroccan Genome Project",
        "priority": "secondary",
        "country_or_region": "Morocco",
        "access_model": "controlled_access",
        "process_guide": "ega",
        "data_scope": "EGA metadata list 109 samples with whole-genome sequencing files for a Moroccan cohort.",
        "why_we_need_it": "Useful to add North African representation, but much smaller than the Emirati and Qatari controlled datasets.",
        "official_release_evidence": "EGA study metadata reviewed 2026-03-13 list publication on 2025-04-25.",
        "build_or_coordinate_note": "The public search result confirms WGS files in EGA; detailed build confirmation should be checked after opening the authorized metadata or dataset page during request prep.",
        "official_links": [
            {
                "label": "EGA study EGAS50000000550",
                "url": "https://ega-archive.org/studies/EGAS50000000550/",
            },
            {
                "label": "EGA DAC EGAC50000000353",
                "url": "https://ega-archive.org/dacs/EGAC50000000353/",
            },
            {
                "label": "EGA request guide",
                "url": "https://ega-archive.org/access/request-data/how-to-request-data/",
            },
        ],
        "access_steps": [
            "Register in EGA and open study EGAS50000000550.",
            "Submit a DAC request naming the planned allele-frequency harmonization and non-identifiable downstream outputs.",
            "Follow the standard EGA DAC communication path until approval or rejection is recorded.",
        ],
        "practical_decision": "Worth pursuing only after the larger Emirati and Qatari routes because the cohort is modest.",
    },
    {
        "key": "tunisian_wes_disorders",
        "display_name": "Tunisian WES cohort",
        "priority": "secondary",
        "country_or_region": "Tunisia",
        "access_model": "controlled_access",
        "process_guide": "ega",
        "data_scope": "EGA public metadata list 75 exome-sequenced participants connected to a rare-disease study.",
        "why_we_need_it": "Potentially useful for rare-variant contextual evidence, but it is not a clean population-frequency baseline and should not be treated like SHGP or gnomAD.",
        "official_release_evidence": "EGA public metadata reviewed 2026-03-13 list publication on 2025-07-07.",
        "build_or_coordinate_note": "Public metadata describe WES files; cohort design indicates disease-enriched context rather than a general-population reference set.",
        "official_links": [
            {
                "label": "EGA study EGAS50000001064",
                "url": "https://ega-archive.org/studies/EGAS50000001064/",
            },
            {
                "label": "EGA request guide",
                "url": "https://ega-archive.org/access/request-data/how-to-request-data/",
            },
        ],
        "access_steps": [
            "Use the standard EGA request path but state clearly that this cohort is disease-focused and would be used only for carefully labelled secondary evidence.",
            "Do not mix its counts directly with population-frequency cohorts without an explicit bias-control policy.",
        ],
        "practical_decision": "Low priority for the main frequency layer because the cohort is small and not population-like.",
    },
    {
        "key": "egyptref_healthy_volunteers",
        "display_name": "EgyptRef healthy-volunteer cohort",
        "priority": "secondary",
        "country_or_region": "Egypt",
        "access_model": "controlled_access",
        "process_guide": "ega",
        "data_scope": "EGA public metadata list 110 Egyptian healthy volunteers with sequencing files.",
        "why_we_need_it": "Provides Egyptian representation, but the cohort is older and smaller than the highest-value targets now available.",
        "official_release_evidence": "EGA public metadata reviewed 2026-03-13 list publication on 2020-08-12.",
        "build_or_coordinate_note": "Public metadata confirm healthy-volunteer cohort structure; detailed build checks should be confirmed during request preparation.",
        "official_links": [
            {
                "label": "EGA dataset EGAD00001001380",
                "url": "https://ega-archive.org/datasets/EGAD00001001380/",
            },
            {
                "label": "EGA DAC EGAC00001000205",
                "url": "https://ega-archive.org/dacs/EGAC00001000205/",
            },
            {
                "label": "EGA request guide",
                "url": "https://ega-archive.org/access/request-data/how-to-request-data/",
            },
        ],
        "access_steps": [
            "Use the standard EGA request path with a clear statement that the cohort is being requested for Arab frequency-comparison work, not for case-level inference.",
            "Confirm build and file layout during the request stage before planning any harmonization work.",
        ],
        "practical_decision": "Useful as supplemental Levant/North Africa context only after higher-yield sources are exhausted.",
    },
)


BROWSE_ONLY_SOURCES: Final[tuple[dict[str, str], ...]] = (
    {
        "display_name": "Almena",
        "status": "browse_only",
        "summary": "Useful for manual cross-checking, but a verified bulk public download path was not confirmed during this review.",
        "url": "https://clingen.igib.res.in/almena/",
    },
)


def build_payload() -> dict[str, object]:
    generated_at = dt.datetime.now(dt.UTC).isoformat()
    return {
        "generated_at": generated_at,
        "scope_note": "This section tracks high-value Arab and Arab-adjacent datasets that are not directly downloadable. It records only source-backed access instructions reviewed from official provider pages.",
        "decision_note": "Priority is driven by cohort scale, coordinate readiness, and value for population-frequency evidence. Controlled datasets are not merged into the pipeline until approval is granted and the raw bytes are frozen.",
        "process_guides": list(PROCESS_GUIDES),
        "sources": list(CONTROLLED_ACCESS_SOURCES),
        "browse_only_sources": list(BROWSE_ONLY_SOURCES),
    }


def main() -> None:
    payload = build_payload()
    UI_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    print(f"wrote_controlled_access_payload={UI_FILE}")
    print(f"source_count={len(payload['sources'])}")


if __name__ == "__main__":
    main()

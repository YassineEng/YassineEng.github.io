---
date: 2025-11-13 08:20:35 +0300
title: IX. Byte Pair Encoding (BPE) Tokenizer from Scratch
github_url: https://github.com/YassineEng/Byte-pair-encoding-Tokenizer-from-scratch
order: 9
---

<!-- Badges (must be outside YAML front matter) -->
<div style="margin-left: 20px;">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?logo=python">
  <img src="https://img.shields.io/badge/Rust-Parser-orange?logo=rust">
  <img src="https://img.shields.io/badge/PyO3-Integration-red">
</div>

<p style="margin-left: 20px;">This project is an educational journey to build a Unicode character property database and a Byte Pair Encoding (BPE) tokenizer from first principles in Python. It aims to replicate core functionalities found in Python's unicodedata module and modern BPE tokenization libraries, providing a deep understanding of Unicode handling and text processing. To enhance performance in critical sections, a Rust-based parser is integrated using PyO3. A Rust free version is available on the second branch of the repo.</p>

<!-- Optional: absolute path for GitHub Pages deployment -->
<!--
<div style="text-align: center;">
  <video width="600" autoplay loop muted playsinline controls>
    <source src="https://yassineeng.github.io/images/your-video.mp4" type="video/mp4">
    Your browser does not support the video tag.
  </video>
</div>
-->

<div style="overflow: auto; height: 400px; background-color: #f6f8fa; padding: 15px; border-radius: 5px; margin-top: 20px;">
<pre>
# Byte Pair Encoding (BPE) Tokenizer from Scratch

This project is an educational journey to build a Unicode character property database and a Byte Pair Encoding (BPE) tokenizer from first principles in Python. It aims to replicate core functionalities found in Python's `unicodedata` module and modern BPE tokenization libraries, providing a deep understanding of Unicode handling and text processing. To enhance performance in critical sections, a Rust-based parser is integrated using `PyO3`. A Rust free version is available on the second branch of the repo.

Each `step_XX_*.py` file represents a distinct stage in building this system, progressively adding complexity and functionality.

## Project Structure

```
.
├───.gitignore
├───.python-version
├───LICENSE
├───PropList-17.0.0.txt
├───pyproject.toml
├───README.md
├───requirements.txt
├───Scripts-17.0.0.txt
├───unicode_database.bin
├───outputs\
│   └───parsed_chars.bin
└───src\
    ├───config.py
    ├───step_01_download_data.py
    ├───step_02_parse_data.py
    ├───step_03_indexing.py
    ├───step_04_database_builder.py
    ├───step_05_lookup.py
    ├───step_06_normalizer.py
    ├───step_07_utf8_codec.py
    ├───step_08_build_vocab.py
    ├───step_09_get_pairs.py
    ├───step_10_bpe_encoder.py
    ├───step_11_main.py
    ├───rust_parser\
    │   ├───Cargo.toml
    │   └───src\
    │       └───lib.rs
    └───analysis_tools\
        ├───analyze_random_unicode_data.py
        ├───test_step_01_download_data.py
        ├───... (other test files)
        └───visualize_database.py
```

## Getting Started

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/Unicode.git
    cd Unicode
    ```
2.  **Set up a virtual environment (recommended):**
    ```bash
    python -m venv .venv
    # On Windows
    .venv\Scripts\activate
    # On macOS/Linux
    source .venv/bin/activate
    ```
3.  **Install dependencies and build Rust extension:**
    ```bash
    pip install -r requirements.txt
    # Build and install the Rust parser
    cd src/rust_parser
    maturin develop
    cd ../..
    ```
4.  **Run the main demonstration script:**
    ```bash
    python -m src.step_11_main
    ```
    This script will execute all steps, download necessary data, build the database, and demonstrate the BPE encoder.

## Core Modules and Functionality

Contains global configuration variables for the project, such as the target Unicode version and the BPE training corpus.

*   **`UNICODE_VERSION`**: A string specifying the Unicode version to be used (e.g., "17.0.0").
*   **`BPE_TRAINING_CORPUS`**: A multi-line string containing the text used to train the BPE encoder. This can be modified to experiment with different training data.

### Full Demonstration Output

This section provides a condensed view of the output when running `python -m src.step_11_main`, showcasing the successful execution and key results from each step.

```
================================================================================
UNICODE BYTE PAIR ENCODER BUILD FROM SCRATCH - FULL DEMONSTRATION
================================================================================

==================================================
TESTING STEP 01: DOWNLOAD UNICODE DATA
==================================================
Attempting to download UnicodeData.txt version 17.0.0 (will use existing if present)...
Using existing file: UnicodeData-17.0.0.txt
✓ Successfully downloaded UnicodeData-17.0.0.txt.
  File size: 2198209 bytes.
==================================================
STEP 01 TEST COMPLETED SUCCESSFULLY!
==================================================
Step 01 completed in 0.00 seconds.


--- Testing get_parsed_unicode_chars function ---
Using existing file: UnicodeData-17.0.0.txt
Loading parsed Unicode characters from cache: outputs/parsed_chars.bin
✓ Successfully parsed 795 characters.
  Code point range: U+0000 - U+FB4F
--- parse_unicode_data function tests passed! ---
Step 02 completed in 0.00 seconds.


============================================================
INITIALIZING SHARED UNICODE DATABASE
============================================================
Loading Unicode database from cache: unicode_database.bin
✓ Database loaded successfully.
✓ Unicode Database initialized successfully.
Database initialization completed in 0.00 seconds.


============================================================
INITIALIZING SHARED UNICODE NORMALIZER
============================================================
============================================================
STEP 3: UNICODE NORMALIZATION AND LOOKUP FUNCTIONS
Replicating unicodedata.normalize() and unicodedata.lookup()
============================================================
Building normalization engine...
Building decomposition tables...
✓ Decomposition mappings: 252
  - Canonical: 130
  - Compatibility: 122
Building composition tables...
✓ Composition pairs: 125
============================================================
STEP 3 COMPLETED SUCCESSFULLY!
✓ Implemented Unicode normalization (NFC, NFD, NFKC, NFKD)
✓ Ready for Unicode version support!
============================================================
✓ Unicode Normalizer initialized successfully.
Normalizer initialization completed in 0.00 seconds.


==================================================
TESTING STEP 03: UNICODE INDEXING SYSTEM
==================================================
Testing character retrieval via index:
  U+0041 ('A'): Found 'LATIN CAPITAL LETTER A'
  U+00E9 ('é'): Found 'LATIN SMALL LETTER E WITH ACUTE'
  U+20AC ('€'): Found 'EURO SIGN'
==================================================
STEP 03 TEST COMPLETED SUCCESSFULLY!
==================================================
Step 03 completed in 0.00 seconds.


==================================================
TESTING STEP 04: UNICODE DATABASE BUILDER
==================================================
Removing existing cache file: unicode_database.bin
--- First build (should build from scratch) ---
Cache file not found. Building database from scratch...
Building double-indexed Unicode database...
✓ Created 471 unique character records
✓ Index1 size: 272 entries
✓ Index2 size: 12288 entries
✓ Unique index2 blocks: 3
✓ Built double index: 272 index1 entries, 12288 index2 entries
Caching new database to: unicode_database.bin
✓ First database build successful.
  Database version: 17.0.0
  Total characters: 795
--- Second build (should load from cache) ---
Loading Unicode database from cache: unicode_database.bin
✓ Database loaded successfully.
✓ Second database build successful (should be from cache).
==================================================
STEP 04 TEST COMPLETED SUCCESSFULLY!
==================================================
Step 04 completed in 0.03 seconds.


==================================================
TESTING STEP 05: UNICODE LOOKUP FUNCTIONS
==================================================
--- Testing 'A' (U+0041) ---
  Name: Expected='LATIN CAPITAL LETTER A', Actual='LATIN CAPITAL LETTER A' {✓}
  Category: Expected='Lu', Actual='Lu' {✓}
--- Testing '½' (U+00BD) ---
  Name: Expected='VULGAR FRACTION ONE HALF', Actual='VULGAR FRACTION ONE HALF' {✓}
  Numeric: Expected='0.5', Actual='0.5' {✓}
  Decomp: Expected='<fraction> 0031 2044 0032', Actual='<fraction> 0031 2044 0032' {✓}
==================================================
STEP 05 TEST COMPLETED SUCCESSFULLY!
==================================================
Step 05 completed in 0.00 seconds.


==================================================
TESTING STEP 06: UNICODE NORMALIZATION FUNCTIONS
==================================================
--- Testing 'café' with form 'NFC' ---
  Input: 'café'
  Form: NFC
  Expected: 'café' (Code Points: ['U+0063', 'U+0061', 'U+0066', 'U+00E9'])
  Actual:   'café' (Code Points: ['U+0063', 'U+0061', 'U+0066', 'U+00E9'])
  ✓ Test Passed
--- Testing 'Ü' with form 'NFD' ---
  Input: 'Ü'
  Form: NFD
  Expected: 'Ü' (Code Points: ['U+0055', 'U+0308'])
  Actual:   'Ü' (Code Points: ['U+0055', 'U+0308'])
  ✓ Test Passed
==================================================
STEP 06 TEST COMPLETED SUCCESSFULLY!
==================================================
Step 06 completed in 0.01 seconds.


============================================================
STEP 07: CUSTOM UTF-8 ENCODER/DECODER
============================================================
CUSTOM UTF-8 ENCODER/DECODER TEST
==================================================
Text: 'Hello, world!'
  Custom bytes: [72, 101, 108, 108, 111, 44, 32, 119, 111, 114, 108, 100, 33]
  Custom decoded: 'Hello, world!'
  Round-trip works: True
==================================================
COMPARISON WITH PYTHON BUILT-IN
==================================================
'€':
  Our bytes:    [226, 130, 172]
  Python bytes: [226, 130, 172]
  Match: True
  Our decode: '€'
  Python decode: '€'
  Both work: True
Step 07 completed in 0.00 seconds.


==================================================
TESTING STEP 08: BUILD INITIAL BPE VOCABULARY
==================================================
✓ Initial vocabulary built successfully.
  Total vocabulary size: 258
  Expected size: 258
Sample of vocabulary (Token ID -> Bytes):
  0: b'<|endoftext|>' ('<|endoftext|>')
  1: b'<|unk|>' ('<|unk|>')
  2: b'\x00' ('')
==================================================
STEP 08 TEST COMPLETED SUCCESSFULLY!
==================================================
Step 08 completed in 0.00 seconds.


==================================================
TESTING STEP 09: GET BYTE PAIRS
==================================================
--- Testing with tokens: [1, 2, 3, 1, 2] ---
  Expected pairs: {(1, 2): 2, (2, 3): 1, (3, 1): 1}
  Actual pairs:   {(1, 2): 2, (2, 3): 1, (3, 1): 1}
  ✓ Test Passed
==================================================
STEP 09 TEST COMPLETED SUCCESSFULLY!
==================================================
Step 09 completed in 0.00 seconds.


============================================================
STEP 10: BPE ENCODER CORE LOGIC
============================================================
==================================================
BPE TRAINING AND ENCODING/DECODING DEMONSTRATION
==================================================
Training BPE on corpus of 15 documents with 200 merges...
Initial vocabulary size: 258
Starting BPE training with target number of merges: 200
Corpus pre-tokenized into 134 documents.
Merge 1: (103, 34) -> 258 (bytes: b'e ') (Freq: 244)
Merge 2: (117, 34) -> 259 (bytes: b's ') (Freq: 144)
Merge 3: (118, 106) -> 260 (bytes: b'th') (Freq: 138)
...
Merge 198: (414, 336) -> 455 (bytes: b'after ') (Freq: 5)
Merge 199: (260, 320) -> 456 (bytes: b'than ') (Freq: 5)
Merge 200: (121, 106) -> 457 (bytes: b'wh') (Freq: 5)
  Current merges: 200/200
BPE training complete. Final vocab size: 458
Total merges learned: 200

================================================================================
ALL STEPS AND DEMONSTRATIONS COMPLETED! Total time: 0.36 seconds.
================================================================================
</pre>
</div>
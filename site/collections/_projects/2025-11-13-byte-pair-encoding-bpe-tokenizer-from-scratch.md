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

<p style="margin-left: 20px;">This project is the first step toward implementing the <strong>"Attention is all you need"</strong> scientific paper it incorporates all the Unicodedata python library functions, it also have a UTF-8 encoder decoder coded from scratch. The input prompt to the LLM goes through multiple operations before it is Tokenized, the code implements all those pretreatment and close with a <strong>Byte pair encoding (BPE) Tokenizer</strong>. To enhance parsing performance and reduce processing time, a Rust-based parser is integrated using PyO3. A Rust free version is available on the second branch of the repo.</p>

<!-- Optional: absolute path for GitHub Pages deployment -->
<!--
<div style="text-align: center;">
  <video width="600" autoplay loop muted playsinline controls>
    <source src="https://yassineeng.github.io/images/your-video.mp4" type="video/mp4">
    Your browser does not support the video tag.
  </video>
</div>
-->

<div style="max-height: 600px; overflow-y: auto; border: 2px solid #4CAF50; border-radius: 10px; padding: 20px; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
  
  <!-- Header -->
  <div style="background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%); color: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; text-align: center;">
    <h1 style="margin: 0; font-size: 24px;">🚀 Byte Pair Encoding Tokenizer - Complete Documentation</h1>
    <p style="margin: 5px 0 0 0; opacity: 0.9;">Build from Scratch with Python & Rust</p>
  </div>

  <!-- Introduction -->
  <div style="background: #e8f5e8; padding: 15px; border-left: 4px solid #4CAF50; border-radius: 5px; margin-bottom: 20px;">
    <p style="margin: 0; color: #2c3e50; font-size: 16px; line-height: 1.5;">
      <strong>✨ Each <code style="background: #2c3e50; color: white; padding: 2px 6px; border-radius: 3px;">step_XX_*.py</code> file represents a distinct stage in building this system, progressively adding complexity and functionality.</strong>
    </p>
  </div>

  <!-- Project Structure -->
  <div style="background: white; border-radius: 8px; padding: 15px; margin-bottom: 20px; border: 1px solid #e0e0e0;">
    <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 8px; margin-top: 0;">📁 Project Structure</h2>
    <div style="background: #2c3e50; color: #ecf0f1; padding: 15px; border-radius: 5px; font-family: 'Courier New', monospace; font-size: 14px; line-height: 1.4;">
      <pre style="margin: 0; color: #ecf0f1; white-space: pre-wrap;">
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
        └───visualize_database.py</pre>
    </div>
  </div>

  <!-- Getting Started -->
  <div style="background: white; border-radius: 8px; padding: 15px; margin-bottom: 20px; border: 1px solid #e0e0e0;">
    <h2 style="color: #2c3e50; border-bottom: 2px solid #e74c3c; padding-bottom: 8px; margin-top: 0;">🚀 Getting Started</h2>
    <div style="background: #fff3cd; border: 1px solid #ffeaa7; border-radius: 5px; padding: 15px;">
      <ol style="color: #2c3e50; line-height: 1.6;">
        <li style="margin-bottom: 10px;"><strong>Clone the repository:</strong><br>
          <code style="background: #2c3e50; color: #ecf0f1; padding: 8px 12px; border-radius: 4px; display: inline-block; margin-top: 5px; font-family: 'Courier New', monospace;">
            git clone https://github.com/YassineEng/Byte-pair-encoding-Tokenizer-from-scratch<br>
          </code>
        </li>
        <li style="margin-bottom: 10px;"><strong>Set up a virtual environment (recommended):</strong><br>
          <code style="background: #2c3e50; color: #ecf0f1; padding: 8px 12px; border-radius: 4px; display: inline-block; margin-top: 5px; font-family: 'Courier New', monospace;">
            python -m venv .venv<br>
            # On Windows: .venv\Scripts\activate<br>
            # On macOS/Linux: source .venv/bin/activate
          </code>
        </li>
        <li style="margin-bottom: 10px;"><strong>Install dependencies and build Rust extension:</strong><br>
          <code style="background: #2c3e50; color: #ecf0f1; padding: 8px 12px; border-radius: 4px; display: inline-block; margin-top: 5px; font-family: 'Courier New', monospace;">
            pip install -r requirements.txt<br>
            cd src/rust_parser<br>
            maturin develop<br>
            cd ../..
          </code>
        </li>
        <li><strong>Run the main demonstration script:</strong><br>
          <code style="background: #2c3e50; color: #ecf0f1; padding: 8px 12px; border-radius: 4px; display: inline-block; margin-top: 5px; font-family: 'Courier New', monospace;">
            python -m src.step_11_main
          </code>
        </li>
      </ol>
      <p style="color: #2c3e50; margin-top: 10px; font-style: italic;">
        This script will execute all steps, download necessary data, build the database, and demonstrate the BPE encoder.
      </p>
    </div>
  </div>

  <!-- Core Modules Header -->
  <div style="background: white; border-radius: 8px; padding: 15px; margin-bottom: 20px; border: 1px solid #e0e0e0;">
    <h2 style="color: #2c3e50; border-bottom: 2px solid #9b59b6; padding-bottom: 8px; margin-top: 0;">🔧 Core Modules and Functionality</h2>
    <div style="background: #e8f4f8; border-radius: 5px; padding: 12px; margin-bottom: 10px;">
      <p style="color: #2c3e50; margin: 0;">Contains global configuration variables for the project, such as the target Unicode version and the BPE training corpus.</p>
      <ul style="color: #2c3e50; margin: 10px 0 0 20px;">
        <li><strong><code>UNICODE_VERSION</code></strong>: A string specifying the Unicode version to be used (e.g., "17.0.0").</li>
        <li><strong><code>BPE_TRAINING_CORPUS</code></strong>: A multi-line string containing the text used to train the BPE encoder. This can be modified to experiment with different training data.</li>
      </ul>
    </div>
  </div>

  <!-- Full Demonstration Output -->
  <div style="background: white; border-radius: 8px; padding: 15px; margin-bottom: 20px; border: 1px solid #e0e0e0;">
    <h2 style="color: #2c3e50; border-bottom: 2px solid #f39c12; padding-bottom: 8px; margin-top: 0;">📊 Full Demonstration Output</h2>
    <div style="background: #2c3e50; color: #ecf0f1; padding: 15px; border-radius: 5px; font-family: 'Courier New', monospace; font-size: 12px; line-height: 1.3; max-height: 300px; overflow-y: auto;">
      <pre style="margin: 0; color: #ecf0f1;">
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
================================================================================</pre>
    </div>
  </div>

  <!-- Individual Module Sections -->
  <!-- Step 01 -->
  <div style="background: #e8f4f8; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #2980b9;">
    <h4 style="color: #2980b9; margin-top: 0;">⬇️ <code>src/step_01_download_data.py</code></h4>
    <p style="color: #2c3e50;">Handles the downloading of the <code>UnicodeData.txt</code> file from the official Unicode website.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Function:</strong> <code>download_unicode_data(unicode_version: str = UNICODE_VERSION) -> str</code><br>
      <strong>Description:</strong> Downloads UnicodeData.txt for specified version. Uses existing file if present.<br>
      <strong>Example:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        from src.step_01_download_data import download_unicode_data<br>
        filename = download_unicode_data("15.0.0")<br>
        print(f"Downloaded data to: {filename}")
      </code>
    </div>
  </div>

  <!-- Step 02 -->
  <div style="background: #fff3e0; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #f39c12;">
    <h4 style="color: #f39c12; margin-top: 0;">⚡ <code>src/step_02_parse_data.py</code></h4>
    <p style="color: #2c3e50;"><strong>Rust-powered parser</strong> for high-performance Unicode data parsing with PyO3 integration.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Class:</strong> <code>UnicodeChar</code> (from Rust via PyO3)<br>
      <strong>Function:</strong> <code>get_parsed_unicode_chars(filename: str, version: str) -> Dict[int, UnicodeChar]</code><br>
      <strong>Features:</strong> Caching, Rust performance, structured character objects<br>
      <strong>Example:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        from src.step_02_parse_data import get_parsed_unicode_chars<br>
        parsed_chars = get_parsed_unicode_chars(data_file, UNICODE_VERSION)<br>
        print(f"Parsed {len(parsed_chars)} Unicode characters using Rust parser.")
      </code>
    </div>
  </div>

  <!-- Step 03 -->
  <div style="background: #e8f5e8; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #27ae60;">
    <h4 style="color: #27ae60; margin-top: 0;">📊 <code>src/step_03_indexing.py</code></h4>
    <p style="color: #2c3e50;">Implements two-level indexing system for O(1) character property lookup.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Class:</strong> <code>DoubleIndexedUnicodeDatabase</code><br>
      <strong>Features:</strong> index1 & index2 arrays, efficient O(1) lookup, mirrors Python C implementation<br>
      <strong>Methods:</strong> _build_double_index(), _get_record_index(), get_character_by_index()
    </div>
  </div>

  <!-- Step 04 -->
  <div style="background: #f3e5f5; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #8e44ad;">
    <h4 style="color: #8e44ad; margin-top: 0;">🏗️ <code>src/step_04_database_builder.py</code></h4>
    <p style="color: #2c3e50;">Primary entry point for building and caching the complete Unicode database.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Function:</strong> <code>build_database() -> UnicodeDatabaseWithIndex</code><br>
      <strong>Features:</strong> Disk caching, automatic rebuilding, version checking<br>
      <strong>Example:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        from src.step_04_database_builder import build_database<br>
        db = build_database()<br>
        print(f"Database for Unicode version: {db.version}")
      </code>
    </div>
  </div>

  <!-- Step 05 -->
  <div style="background: #e3f2fd; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #1565c0;">
    <h4 style="color: #1565c0; margin-top: 0;">🔍 <code>src/step_05_lookup.py</code></h4>
    <p style="color: #2c3e50;">Comprehensive Unicode character property lookup functions.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Class:</strong> <code>UnicodeDatabaseWithIndex</code><br>
      <strong>Methods:</strong> name(), category(), decimal(), digit(), numeric(), combining(), bidirectional(), mirrored(), decomposition()<br>
      <strong>Example:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        char_euro = '€'<br>
        print(f"Name: {db.name(char_euro)}")<br>
        print(f"Category: {db.category(char_euro)}")
      </code>
    </div>
  </div>

  <!-- Step 06 -->
  <div style="background: #e8f5e8; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #2e7d32;">
    <h4 style="color: #2e7d32; margin-top: 0;">🔄 <code>src/step_06_normalizer.py</code></h4>
    <p style="color: #2c3e50;">Implements Unicode normalization forms (NFC, NFD, NFKC, NFKD).</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Class:</strong> <code>UnicodeNormalizer</code><br>
      <strong>Factory:</strong> <code>create_normalizer() -> UnicodeNormalizer</code><br>
      <strong>Methods:</strong> normalize(), is_normalized(), _decompose(), _compose()<br>
      <strong>Example:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        normalizer = create_normalizer()<br>
        normalized = normalizer.normalize("café", 'NFC')<br>
        print(f"Normalized: {normalized}")
      </code>
    </div>
  </div>

  <!-- Step 07 -->
  <div style="background: #fff3e0; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #ef6c00;">
    <h4 style="color: #ef6c00; margin-top: 0;">💾 <code>src/step_07_utf8_codec.py</code></h4>
    <p style="color: #2c3e50;">Custom UTF-8 encoder/decoder implementation.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Class:</strong> <code>CustomUTF8Codec</code> (static methods)<br>
      <strong>Methods:</strong> encode(), decode(), _encode_code_point()<br>
      <strong>Example:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        encoded = CustomUTF8Codec.encode("Hello café €")<br>
        decoded = CustomUTF8Codec.decode(encoded)<br>
        print(f"Encoded: {encoded}")
      </code>
    </div>
  </div>

  <!-- Step 08 -->
  <div style="background: #fce4ec; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #c2185b;">
    <h4 style="color: #c2185b; margin-top: 0;">📚 <code>src/step_08_build_vocab.py</code></h4>
    <p style="color: #2c3e50;">Builds initial BPE vocabulary with 256 byte values + special tokens.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Function:</strong> <code>build_initial_vocab() -> Tuple[Dict[bytes, int], Dict[int, bytes]]</code><br>
      <strong>Returns:</strong> vocab mapping and reverse mapping<br>
      <strong>Example:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        vocab, token_to_bytes = build_initial_vocab()<br>
        print(f"Vocab size: {len(vocab)}")<br>
        print(f"Token for 'a': {vocab[b'a']}")
      </code>
    </div>
  </div>

  <!-- Step 09 -->
  <div style="background: #e0f2f1; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #00695c;">
    <h4 style="color: #00695c; margin-top: 0;">🔄 <code>src/step_09_get_pairs.py</code></h4>
    <p style="color: #2c3e50;">Identifies frequent byte pairs during BPE training.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Function:</strong> <code>get_byte_pairs(tokens: List[int]) -> Dict[Tuple[int, int], int]</code><br>
      <strong>Purpose:</strong> Count consecutive token pairs for BPE training<br>
      <strong>Example:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        pairs = get_byte_pairs([1, 2, 3, 1, 2])<br>
        print(f"Byte pairs: {pairs}")  # {(1,2): 2, (2,3): 1, (3,1): 1}
      </code>
    </div>
  </div>

  <!-- Step 10 -->
  <div style="background: #fff8e1; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #ff8f00;">
    <h4 style="color: #ff8f00; margin-top: 0;">🤖 <code>src/step_10_bpe_encoder.py</code></h4>
    <p style="color: #2c3e50;">Core Byte Pair Encoding algorithm implementation.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Class:</strong> <code>CustomBPEEncoder</code><br>
      <strong>Methods:</strong> train(), encode(), decode(), get_vocab_info(), get_merges_info()<br>
      <strong>Example:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        bpe_encoder = CustomBPEEncoder(normalizer)<br>
        bpe_encoder.train(200)<br>
        encoded = bpe_encoder.encode("hello world")<br>
        decoded = bpe_encoder.decode(encoded)
      </code>
    </div>
  </div>

  <!-- Step 11 -->
  <div style="background: #e8eaf6; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #303f9f;">
    <h4 style="color: #303f9f; margin-top: 0;">🎯 <code>src/step_11_main.py</code></h4>
    <p style="color: #2c3e50;">Main orchestration script - runs full pipeline demonstration.</p>
    <div style="background: #34495e; color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
      <strong>Function:</strong> <code>main()</code><br>
      <strong>Purpose:</strong> Executes complete pipeline from step 01 to 10<br>
      <strong>Usage:</strong><br>
      <code style="display: block; background: #2c3e50; padding: 8px; border-radius: 3px; margin-top: 5px;">
        python -m src.step_11_main
      </code>
    </div>
  </div>

  <!-- Rust Parser -->
  <div style="background: #ffebee; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #c62828;">
    <h3 style="color: #c62828; border-bottom: 2px solid #c62828; padding-bottom: 5px;">🦀 Rust Parser (src/rust_parser/)</h3>
    <p style="color: #2c3e50;"><strong>High-performance Unicode data parser using Rust + PyO3</strong></p>
    <ul style="color: #2c3e50;">
      <li><strong><code>src/rust_parser/src/lib.rs</code></strong>: Core Rust logic with UnicodeChar struct and parse_unicode_data function</li>
      <li><strong><code>src/rust_parser/Cargo.toml</code></strong>: Rust project manifest with PyO3 dependencies</li>
    </ul>
    <p style="color: #2c3e50; font-style: italic;">Provides significant performance benefits over pure Python implementation</p>
  </div>

  <!-- Analysis Tools -->
  <div style="background: #f3e5f5; border-radius: 8px; padding: 15px; margin-bottom: 15px; border-left: 4px solid #7b1fa2;">
    <h3 style="color: #7b1fa2; border-bottom: 2px solid #7b1fa2; padding-bottom: 5px;">🔬 Analysis Tools (src/analysis_tools/)</h3>
    <p style="color: #2c3e50;">Scripts for testing, analysis, and visualization of Unicode components.</p>
    <ul style="color: #2c3e50;">
      <li><strong><code>analyze_random_unicode_data.py</code></strong>: Detailed breakdown of random Unicode character properties</li>
      <li><strong><code>visualize_database.py</code></strong>: Human-readable database visualization and statistics</li>
      <li><strong><code>test_step_XX_*.py</code></strong>: Individual component test files for each step</li>
    </ul>
  </div>

  <!-- Final Footer -->
  <div style="background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%); color: white; padding: 20px; border-radius: 8px; text-align: center; margin-top: 20px;">
    <h3 style="margin: 0 0 10px 0; color: #3498db;">🎉 Project Complete!</h3>
    <p style="margin: 0; opacity: 0.9;">
      <strong>From Unicode Data Download to BPE Encoding - Built Entirely From Scratch</strong><br>
      Featuring High-Performance Rust Integration, Custom UTF-8 Codec, and Complete Unicode Compliance
    </p>
    <div style="margin-top: 15px; display: flex; justify-content: center; gap: 20px; flex-wrap: wrap;">
      <span style="background: #e74c3c; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🦀 Rust Powered</span>
      <span style="background: #3498db; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🐍 Python Integrated</span>
      <span style="background: #27ae60; padding: 5px 10px; border-radius: 15px; font-size: 12px;">📊 Unicode 17.0.0</span>
      <span style="background: #9b59b6; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🤖 BPE Tokenization</span>
    </div>
  </div>

</div>
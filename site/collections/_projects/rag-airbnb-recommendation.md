---
title: III.    RAG-Using-Airbnb-reviews-to-augment-touristic-recommendation-huggingface
date: 2023-10-27
github_url: https://github.com/YassineEng/RAG-Using-Airbnb-reviews-to-augment-touristic-recommendation-huggingface
order: 3
---

<!-- Badges (must be outside YAML front matter) -->
<div style="margin-left: 20px;">
  <img src="https://img.shields.io/badge/Python-3.x-blue?logo=python">
  <img src="https://img.shields.io/badge/LangChain-Framework-green?logo=langchain">
  <img src="https://img.shields.io/badge/FAISS-Vector%20Search-red">
  <img src="https://img.shields.io/badge/Sentence%20Transformers-Embeddings-blueviolet">
  <img src="https://img.shields.io/badge/Hugging%20Face-Transformers-yellow?logo=huggingface">
  <img src="https://img.shields.io/badge/PyTorch-Deep%20Learning-EE4C2C?logo=pytorch">
</div>



<p style="margin-left: 20px;">This project leverages a Retrieval-Augmented Generation (RAG) model to provide touristic recommendations based on a large dataset of Airbnb reviews. By analyzing real guest experiences, the system can answer user queries about cities, neighborhoods, and specific listings, offering nuanced insights that go beyond simple ratings.</p>

<div class="code-window-container">
  <div class="code-window">
    <div class="code-header">
      <span class="red"></span>
      <span class="yellow"></span>
      <span class="green"></span>
    </div>
    <div class="code-body">
<pre><code>
  <span style="color:#8b949e;"># This script defines the core components of the RAG (Retrieval-Augmented
  Generation) pipeline.</span>
  <span style="color:#8b949e;"># It includes functions for loading the generative language model and for answering
  queries</span>
  <span style="color:#8b949e;"># by combining retrieved context with a language model.</span>

  <span style="color:#ff7b72;">from</span> <span style="color:#d2a8ff;">langchain_huggingface</span> <span
  style="color:#ff7b72;">import</span> HuggingFacePipeline
  <span style="color:#ff7b72;">from</span> <span style="color:#d2a8ff;">langchain_core.prompts</span> <span
  style="color:#ff7b72;">import</span> PromptTemplate
  <span style="color:#ff7b72;">from</span> <span style="color:#d2a8ff;">transformers</span> <span
  style="color:#ff7b72;">import</span> AutoTokenizer, AutoModelForCausalLM, pipeline
  <span style="color:#ff7b72;">from</span> <span style="color:#d2a8ff;">src.rag_airbnb_config</span> <span
  style="color:#ff7b72;">import</span> GEN_MODEL
  <span style="color:#ff7b72;">from</span> <span style="color:#d2a8ff;">src.rag_airbnb_faiss_index</span> <span
  style="color:#ff7b72;">import</span> retrieve_from_faiss


  <span style="color:#79c0ff;">def</span> <span style="color:#d2a8ff;">load_hf_model</span>():
      <span style="color:#a5d6ff;">"""Loads the Hugging Face generative model and tokenizer."""</span>
      <span style="color:#8b949e;"># Initialize the tokenizer for the generative model.</span>
      tokenizer = AutoTokenizer.from_pretrained(GEN_MODEL)

      <span style="color:#8b949e;"># Load the pre-trained causal language model.</span>
      model = AutoModelForCausalLM.from_pretrained(GEN_MODEL, device_map=<span
  style="color:#a5d6ff;">"auto"</span>, torch_dtype=<span style="color:#a5d6ff;">"auto"</span>)

      <span style="color:#8b949e;"># Create a text generation pipeline.</span>
      text_gen = pipeline(<span style="color:#a5d6ff;">"text-generation"</span>, model=model, tokenizer=tokenizer,
  max_new_tokens=512)

      <span style="color:#8b949e;"># Wrap the pipeline in a LangChain HuggingFacePipeline.</span>
      <span style="color:#ff7b72;">return</span> HuggingFacePipeline(pipeline=text_gen)


  <span style="color:#79c0ff;">def</span> <span style="color:#d2a8ff;">answer_query</span>(query, index, reviews,
  embedder, llm):
      <span style="color:#a5d6ff;">"""Answers a user query using the RAG pipeline."""</span>
      <span style="color:#8b949e;"># 1. Retrieve relevant documents</span>
      context_docs = retrieve_from_faiss(embedder.encode([query], normalize_embeddings=<span
  style="color:#ff7b72;">True</span>), index, reviews, embedder)

      <span style="color:#8b949e;"># 2. Print summary</span>
      <span style="color:#ff7b72;">print</span>(<span style="color:#a5d6ff;">"\n--- Retrieved Context (Summary)
  ---"</span>)
      <span style="color:#ff7b72;">if</span> context_docs:
          <span style="color:#ff7b72;">for</span> i, doc <span style="color:#ff7b72;">in</span> <span
  style="color:#d2a8ff;">enumerate</span>(context_docs):
              <span style="color:#ff7b72;">print</span>(f<span style="color:#a5d6ff;">"Doc {i+1}: {doc.get('text',
  '')[:100]}..."</span>)
      <span style="color:#ff7b72;">else</span>:
          <span style="color:#ff7b72;">print</span>(<span style="color:#a5d6ff;">"No relevant documents
  retrieved."</span>)

      <span style="color:#8b949e;"># 3. Join context</span>
      context = <span style="color:#a5d6ff;">"\n\n"</span>.join([f<span style="color:#a5d6ff;">"[{d['listing_id']}]
   {d['text']}"</span> <span style="color:#ff7b72;">for</span> d <span style="color:#ff7b72;">in</span>
  context_docs])

      <span style="color:#8b949e;"># 4. Prompt template</span>
      prompt = PromptTemplate.from_template(
          <span style="color:#a5d6ff;">"""You are a helpful assistant that analyzes Airbnb reviews.
  Context: {context}

  Question: {question}

  Answer:"""</span>
      )

      <span style="color:#8b949e;"># 5. Format prompt</span>
      prompt_text = prompt.format(context=context, question=query)

      <span style="color:#8b949e;"># 6. Generate answer</span>
      <span style="color:#ff7b72;">print</span>(<span style="color:#a5d6ff;">"\n---\n"</span>)
      <span style="color:#ff7b72;">print</span>(llm.invoke(prompt_text))
      <span style="color:#ff7b72;">print</span>(<span style="color:#a5d6ff;">"\n---\n"</span>)
  </code></pre>
    </div>
  </div>

  <div class="code-window">
    <div class="code-header">
      <span class="red"></span>
      <span class="yellow"></span>
      <span class="green"></span>
    </div>
    <div class="code-body">
<pre><code>
  <span style="color:#8b949e;"># This script manages the creation and caching of review embeddings.</span>
  <span style="color:#8b949e;"># It uses a sentence-transformer model to generate embeddings and an SQLite
  database</span>
  <span style="color:#8b949e;"># to cache them, allowing for efficient resume-from-where-you-left-off
  functionality.</span>

  <span style="color:#ff7b72;">from</span> <span style="color:#d2a8ff;">sentence_transformers</span> <span
  style="color:#ff7b72;">import</span> SentenceTransformer
  <span style="color:#ff7b72;">from</span> <span style="color:#d2a8ff;">tqdm</span> <span
  style="color:#ff7b72;">import</span> tqdm
  <span style="color:#ff7b72;">import</span> numpy <span style="color:#ff7b72;">as</span> np
  <span style="color:#ff7b72;">import</span> sqlite3, json, os

  <span style="color:#ff7b72;">from</span> <span style="color:#d2a8ff;">src.rag_airbnb_config</span> <span
  style="color:#ff7b72;">import</span> EMBED_MODEL, SQLITE_PATH, ID_COLUMN, EMBEDDING_DIM, BATCH_SIZE
  <span style="color:#ff7b72;">from</span> <span style="color:#d2a8ff;">src.rag_airbnb_database</span> <span
  style="color:#ff7b72;">import</span> load_reviews


  <span style="color:#8b949e;"># ----------------------------------------</span>
  <span style="color:#8b949e;"># Helper Functions for SQLite Caching</span>
  <span style="color:#8b949e;"># ----------------------------------------</span>

  <span style="color:#79c0ff;">def</span> <span style="color:#d2a8ff;">init_sqlite</span>():
      <span style="color:#a5d6ff;">"""Initializes the SQLite database and creates the embeddings table if it
  doesn't exist."""</span>
      conn = sqlite3.connect(SQLITE_PATH)
      conn.execute(f<span style="color:#a5d6ff;">"""
          CREATE TABLE IF NOT EXISTS embeddings (
              {ID_COLUMN} TEXT PRIMARY KEY,
              review_text TEXT,
              embedding BLOB
          )
      """</span>)
      conn.commit()
      <span style="color:#ff7b72;">return</span> conn


  <span style="color:#79c0ff;">def</span> <span style="color:#d2a8ff;">get_existing_ids</span>(sqlite_conn):
      <span style="color:#a5d6ff;">"""Retrieves IDs of all reviews already embedded and cached."""</span>
      cur = sqlite_conn.cursor()
      cur.execute(f<span style="color:#a5d6ff;">"SELECT {ID_COLUMN} FROM embeddings"</span>)
      ids = {r[0] <span style="color:#ff7b72;">for</span> r <span style="color:#ff7b72;">in</span> cur.fetchall()}
      <span style="color:#ff7b72;">return</span> ids


  <span style="color:#79c0ff;">def</span> <span style="color:#d2a8ff;">save_embedding_to_sqlite</span>(sqlite_conn,
   review_id, review_text, embedding):
      <span style="color:#a5d6ff;">"""Saves one embedding to the SQLite cache."""</span>
      sqlite_conn.execute(f<span style="color:#a5d6ff;">"""
          INSERT OR REPLACE INTO embeddings ({ID_COLUMN}, review_text, embedding)
          VALUES (?, ?, ?)
      """</span>, (review_id, review_text, json.dumps(embedding.tolist())))
      sqlite_conn.commit()


  <span style="color:#79c0ff;">def</span> <span
  style="color:#d2a8ff;">load_all_embeddings_from_sqlite</span>(sqlite_conn):
      <span style="color:#a5d6ff;">"""Loads all embeddings from SQLite cache."""</span>
      cur = sqlite_conn.cursor()
      cur.execute(f<span style="color:#a5d6ff;">"SELECT {ID_COLUMN}, review_text, embedding FROM embeddings ORDER
  BY {ID_COLUMN}"</span>)
      all_data = []
      <span style="color:#ff7b72;">for</span> row <span style="color:#ff7b72;">in</span> cur.fetchall():
          review_id, review_text, embedding_blob = row
          embedding = np.array(json.loads(embedding_blob), dtype=np.float32)
          all_data.append({<span style="color:#a5d6ff;">"review_id"</span>: review_id, <span
  style="color:#a5d6ff;">"text"</span>: review_text, <span style="color:#a5d6ff;">"embedding"</span>: embedding})
      <span style="color:#ff7b72;">return</span> all_data


  <span style="color:#8b949e;"># ----------------------------------------</span>
  <span style="color:#8b949e;"># Main Embedding Pipeline</span>
  <span style="color:#8b949e;"># ----------------------------------------</span>

  <span style="color:#79c0ff;">def</span> <span
  style="color:#d2a8ff;">build_embeddings_with_sqlite</span>(all_reviews):
      <span style="color:#a5d6ff;">"""Builds embeddings for reviews using SQLite cache."""</span>
      <span style="color:#ff7b72;">print</span>(<span style="color:#a5d6ff;">"Starting embedding pipeline with
  SQLite cache..."</span>)
      sqlite_conn = init_sqlite()
      existing_ids = get_existing_ids(sqlite_conn)
      <span style="color:#ff7b72;">print</span>(f<span style="color:#a5d6ff;">"Found {len(existing_ids)} existing
  embeddings. Resuming..."</span>)

      embedder = SentenceTransformer(EMBED_MODEL)
      reviews_to_embed = [r <span style="color:#ff7b72;">for</span> r <span style="color:#ff7b72;">in</span>
  all_reviews <span style="color:#ff7b72;">if</span> r[ID_COLUMN] <span style="color:#ff7b72;">not in</span>
  existing_ids]
      total_to_embed = len(reviews_to_embed)

      <span style="color:#ff7b72;">if</span> total_to_embed &gt; 0:
          <span style="color:#ff7b72;">print</span>(f<span style="color:#a5d6ff;">"[+] Creating embeddings for
  {total_to_embed} reviews..."</span>)
          <span style="color:#ff7b72;">for</span> i <span style="color:#ff7b72;">in</span> tqdm(<span
  style="color:#d2a8ff;">range</span>(0, total_to_embed, BATCH_SIZE), desc=<span style="color:#a5d6ff;">"Embedding
  batches"</span>):
              batch = reviews_to_embed[i:i + BATCH_SIZE]
              batch_texts = [r[<span style="color:#a5d6ff;">"text"</span>] <span style="color:#ff7b72;">for</span>
  r <span style="color:#ff7b72;">in</span> batch]
              batch_embeddings = embedder.encode(batch_texts, normalize_embeddings=<span
  style="color:#ff7b72;">True</span>)

              <span style="color:#ff7b72;">for</span> j, r <span style="color:#ff7b72;">in</span> <span
  style="color:#d2a8ff;">enumerate</span>(batch):
                  save_embedding_to_sqlite(sqlite_conn, r[ID_COLUMN], r[<span
  style="color:#a5d6ff;">"text"</span>], batch_embeddings[j])
          <span style="color:#ff7b72;">print</span>(f<span style="color:#a5d6ff;">"[+] Finished embedding
  {total_to_embed} reviews."</span>)
      <span style="color:#ff7b72;">else</span>:
          <span style="color:#ff7b72;">print</span>(<span style="color:#a5d6ff;">"[+] No new reviews to
  embed."</span>)

      all_embedded_data = load_all_embeddings_from_sqlite(sqlite_conn)
      sqlite_conn.close()

      embeddings_array = np.array([d[<span style="color:#a5d6ff;">"embedding"</span>] <span
  style="color:#ff7b72;">for</span> d <span style="color:#ff7b72;">in</span> all_embedded_data], dtype=np.float32)
      reviews_for_faiss = [{
          <span style="color:#a5d6ff;">"review_id"</span>: d[ID_COLUMN],
          <span style="color:#a5d6ff;">"listing_id"</span>: next((r[<span
  style="color:#a5d6ff;">"listing_id"</span>] <span style="color:#ff7b72;">for</span> r <span
  style="color:#ff7b72;">in</span> all_reviews <span style="color:#ff7b72;">if</span> r[ID_COLUMN] ==
  d[ID_COLUMN]), <span style="color:#a5d6ff;">""</span>),
          <span style="color:#a5d6ff;">"text"</span>: d[<span style="color:#a5d6ff;">"text"</span>]
      } <span style="color:#ff7b72;">for</span> d <span style="color:#ff7b72;">in</span> all_embedded_data]

      <span style="color:#ff7b72;">return</span> embeddings_array, embedder, reviews_for_faiss
  </code></pre>
    </div>
  </div>
</div>
import streamlit as st
from pyvi import ViTokenizer
import re
import fasttext
import os
fasttext_model_path = os.getenv('FASTTEXT_MODEL_PATH')
fasttext_model = fasttext.load_model(fasttext_model_path)


def preprocess_text(text):
    text = text.lower()
    text = re.sub(r'\d+', '', text)  # Remove digits
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    return ViTokenizer.tokenize(text)  # Tokenize Vietnamese text

# Get similar words from the post


def get_similar_words_from_post(post, target_words, top_n=5, similarity_threshold=0.7):
    processed_text = preprocess_text(post)
    words = list(set(processed_text.split()))  # Unique words

    results = {}
    for word in words:
        for target_word in target_words:
            try:
                # Calculate similarity
                similarity = fasttext_model.get_word_vector(
                    word) @ fasttext_model.get_word_vector(target_word)

                if similarity > similarity_threshold:
                    if target_word not in results:
                        results[target_word] = []
                    results[target_word].append((word, similarity))
            except KeyError:
                continue

    # Sort by similarity score
    for key in results:
        results[key] = sorted(
            results[key], key=lambda x: x[1], reverse=True)[:top_n]

    # Count occurrences in the post
    count = sum(1 for word in processed_text.split() if any(
        word == w[0] for v in results.values() for w in v))
    return results, count


# Streamlit UI
st.title('Vietnamese Text Similarity Analysis')

post = st.text_area("Enter Post Content:")
target_words = st.text_input(
    "Enter Target Words (comma separated):").split(',')
similarity_threshold = st.slider("Similarity Threshold:", 0.0, 1.0, 0.7)
top_n = st.slider("Top N Similar Words:", 1, 20, 5)

if st.button("Analyze"):
    if post and target_words:
        similar_words, count = get_similar_words_from_post(
            post, target_words, top_n, similarity_threshold)
        st.write(f"Number of similar words in post: {count}")

        st.write("Similar Words in the Post:")
        for k, v in similar_words.items():
            st.write(f"{k}: {', '.join([w[0] for w in v])}")
    else:
        st.write("Please enter both post content and target words.")

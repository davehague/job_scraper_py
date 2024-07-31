import pandas as pd
from tqdm import tqdm
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
from nltk.corpus import stopwords
import nltk

# Download required NLTK data
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)


def preprocess_text(text):
    # Convert to lowercase
    text = text.lower()
    # Remove punctuation
    text = re.sub(r'[^\w\s]', '', text)
    # Remove numbers
    text = re.sub(r'\d+', '', text)
    # Remove extra whitespace
    text = ' '.join(text.split())
    return text


def extract_keywords(text):
    # Tokenize the text
    tokens = nltk.word_tokenize(text)
    # Remove stopwords
    stop_words = set(stopwords.words('english'))
    keywords = [word for word in tokens if word not in stop_words]
    return ' '.join(keywords)


def compare_resume_to_job(resume_text, job_title, job_description, similarity_threshold=0.1):
    # Preprocess texts
    resume_text = preprocess_text(resume_text)
    job_title = preprocess_text(job_title)
    job_description = preprocess_text(job_description)

    # Extract keywords
    resume_keywords = extract_keywords(resume_text)
    job_keywords = extract_keywords(job_title + " " + job_description)

    # Combine texts for vectorization
    all_texts = [resume_keywords, job_keywords]

    # Use TF-IDF to vectorize the texts
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_features=5000)
    tfidf_matrix = vectorizer.fit_transform(all_texts)

    # Compute cosine similarity
    cosine_sim = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]

    print(f"Similarity score: {cosine_sim:.4f}")

    # Get top matching features
    feature_names = vectorizer.get_feature_names_out()
    resume_vector = tfidf_matrix[0].toarray()[0]
    job_vector = tfidf_matrix[1].toarray()[0]

    matching_features = []
    for i, (resume_value, job_value) in enumerate(zip(resume_vector, job_vector)):
        if resume_value > 0 and job_value > 0:
            matching_features.append((feature_names[i], min(resume_value, job_value)))

    matching_features.sort(key=lambda x: x[1], reverse=True)

    print("\nTop 10 matching keywords/phrases:")
    for feature, score in matching_features[:10]:
        print(f"  {feature}: {score:.4f}")

    # Find missing important job keywords
    missing_features = []
    for i, job_value in enumerate(job_vector):
        if job_value > similarity_threshold and resume_vector[i] == 0:
            missing_features.append((feature_names[i], job_value))

    missing_features.sort(key=lambda x: x[1], reverse=True)

    print("\nTop 10 important job keywords missing from resume:")
    for feature, score in missing_features[:10]:
        print(f"  {feature}: {score:.4f}")

    return cosine_sim, matching_features, missing_features


def find_top_job_matches(user_resume, all_jobs, top_n=10):
    # Create a list to store results
    results = []

    # Iterate through all jobs with a progress bar
    for index, row in tqdm(all_jobs.iterrows(), total=len(all_jobs), desc="Comparing jobs"):
        job_title = row.get('title', '')
        job_description = row.get('description', '')
        job_url = row.get('job_url', '')

        # Compare resume to job
        similarity, matches, missing = compare_resume_to_job(user_resume, job_title, job_description)

        # Store results
        results.append({
            'job_id': index,
            'job_url': job_url,
            'title': job_title,
            'similarity': similarity,
            'top_matches': ', '.join([match[0] for match in matches[:5]]),
            'top_missing': ', '.join([miss[0] for miss in missing[:5]])
        })

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)

    # Sort by similarity score and get top N matches
    top_matches = results_df.sort_values('similarity', ascending=False).head(top_n)

    return top_matches

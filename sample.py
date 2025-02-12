import sqlite3
import os
import json
import re
from langchain_huggingface import HuggingFaceEndpoint
from typing import TypedDict
from flask import Flask, jsonify

# Set your Hugging Face API Token
os.environ["HUGGINGFACEHUB_API_TOKEN"] = "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"

# Initialize HuggingFaceEndpoint with your model repository
llm = HuggingFaceEndpoint(repo_id="mistralai/Mistral-Nemo-Instruct-2407")

# Define the structure of the result using TypedDict
class PostAnalysisResult(TypedDict):
    post_id: str
    toxicity_score: float
    threat_level_score: float
    non_educational_score: float
    description: str
    message: str

# Initialize Flask app
app = Flask(__name__)

# Function to initialize the database and create the posts table if it doesn't exist
def initialize_db():
    try:
        conn = sqlite3.connect('posts.db')
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT NOT NULL,
                post_content TEXT,
                post_attachment TEXT,
                processed INTEGER DEFAULT 0
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analysis_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT NOT NULL,
                toxicity_score REAL,
                threat_level_score REAL,
                non_educational_score REAL,
                description TEXT,
                message TEXT
            )
        """)

        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error initializing database: {e}")

# Function to query post content from the database
def get_post_content():
    try:
        conn = sqlite3.connect('posts.db')
        cursor = conn.cursor()

        cursor.execute("SELECT post_id, post_content FROM posts WHERE processed = 0 LIMIT 1")
        row = cursor.fetchone()

        conn.close()

        if row:
            return row  # Return both post_id and post_content
        else:
            return None  # No unprocessed posts found

    except Exception as e:
        print(f"Error fetching post content: {e}")
        return None

# Function to query Hugging Face model using LangChain's HuggingFaceEndpoint
# Function to query Hugging Face model using LangChain's HuggingFaceEndpoint
def query_huggingface(post_id, text) -> PostAnalysisResult:
    # Define the prompt with explicit JSON request
    prompt = f"""

            Please analyze the following text with post_id: {post_id} and provide the classification in a structured JSON format.

            1. Toxicity: Does the text contain harmful or offensive language? Respond with a score from 0 to 100.

            2. Threat Level: Does the text contain any threats or incitement to violence? Provide a score from 0 to 100.

            3. Non-Educational Content: Does the text include personal topics or content unrelated to technology (e.g., car-related posts)? Provide a score from 0 to 100.

            Text: "{text}"

            Please return the results as a JSON object with the fields post_id, toxicity_score, threat_level_score, non_educational_score, description, and message. The description should explain the reasoning behind the scores and should not exceed 10 words.

            """



    # Call Hugging Face model
    result = llm.invoke(prompt)
    #print(f"Raw response from Hugging Face model: {result}")  # Debugging line to check raw response

    # Clean the result by removing Python-style comments and extra text
    cleaned_result = result.strip().replace("\n", "").replace("```", "")

    # Use regex to extract the first JSON object
    json_match = re.search(r'\{.*?\}', cleaned_result)  # Greedy match to extract the first JSON object

    if json_match:
        cleaned_result = json_match.group(0)  # Extract the first JSON object

        # Try to parse the cleaned result as JSON
        try:
            result_dict = json.loads(cleaned_result)
            return result_dict
        except json.JSONDecodeError:
            print("Error: Extracted response is not valid JSON.")
            print(f"Cleaned raw response was: {cleaned_result}")  # Print the cleaned raw response for further debugging
            return {}  # Return empty dictionary or handle as necessary
    else:
        print("Error: No JSON object found in response.")
        print(f"Cleaned raw response was: {cleaned_result}")  # Print the cleaned raw response for further debugging
        return {}


# Function to process the scores and generate messages
def process_scores(result, post_id) -> PostAnalysisResult:
    try:
        toxicity_score = round(result.get("toxicity_score", 0), 2)
        threat_level_score = round(result.get("threat_level_score", 0), 2)
        non_educational_score = round(result.get("non_educational_score", 0), 2)
        description = result.get("description", "No description provided.")

        # Decision logic based on toxicity score (or confidence score)
        if toxicity_score >= 70:
            message = "It should be deleted."  # Flag as harmful for deletion
            delete_post(post_id)  # Trigger the DELETE logic
        elif non_educational_score >= 70:
            message = "It should be deleted."
            delete_post(post_id)
        elif threat_level_score >= 70:
            message = "It should be deleted."
            delete_post(post_id)
        else:
            message = "The post can retain."

        result_dict: PostAnalysisResult = {
            "post_id": post_id,
            "toxicity_score": toxicity_score,
            "threat_level_score": threat_level_score,
            "non_educational_score": non_educational_score,
            "description": description,
            "message": message
        }

        return result_dict

    except Exception as e:
        print(f"Error processing result: {e}")
        return {"error": "Failed to process result"}

# Function to insert analysis results into the database
def insert_analysis_results(result: PostAnalysisResult):
    try:
        print(f"Inserting data into database for post_id {result['post_id']}")

        conn = sqlite3.connect('posts.db')
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO analysis_results (post_id, toxicity_score, threat_level_score, non_educational_score, description, message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            result['post_id'],
            result['toxicity_score'],
            result['threat_level_score'],
            result['non_educational_score'],
            result['description'],
            result['message']
        ))

        # Update the post's 'processed' column to 1 to mark it as processed
        cursor.execute("""
            UPDATE posts
            SET processed = 1
            WHERE post_id = ?
        """, (result['post_id'],))

        conn.commit()
        conn.close()

        print(f"Data inserted for post_id {result['post_id']} and marked as processed.")

    except Exception as e:
        print(f"Error inserting analysis results: {e}")

# Function to delete a post
def delete_post(post_id):
    try:
        conn = sqlite3.connect('posts.db')
        cursor = conn.cursor()

        # Delete the post based on post_id
        cursor.execute("DELETE FROM posts WHERE post_id = ?", (post_id,))
        conn.commit()
        conn.close()

        print(f"Post with ID {post_id} deleted successfully.")
    except Exception as e:
        print(f"Error deleting post {post_id}: {e}")

# Main execution
initialize_db()  # Ensure the database is initialized

post_content_data = get_post_content()

if post_content_data:
    post_id, post_content = post_content_data  # Unpack post_id and post_content
    print(f"Analyzing post content: {post_content}")

    # Query Hugging Face model using LangChain and process the result
    result = query_huggingface(post_id, post_content)

    # Process the result and get the structured JSON response
    if result:
        processed_result = process_scores(result, post_id)
        print(f"Processed result: {processed_result}")

        # Insert the processed results into the database
        insert_analysis_results(processed_result)
else:
    print("No unprocessed posts found in the database.")

# DELETE API endpoint
@app.route('/delete_post/<post_id>', methods=['DELETE'])
def delete_post_api(post_id):
    try:
        delete_post(post_id)  # Call the delete_post function
        return jsonify({"message": f"Post with ID {post_id} deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to delete post {post_id}"}), 500


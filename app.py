import sqlite3
from celery import Celery
from flask import Flask, request, jsonify

# Configure Celery
celery_app = Celery('tasks', broker='redis://localhost:6379/0')

# SQLite database path
DATABASE = 'posts.db'

# Initialize Flask app
app = Flask(__name__)

# Initialize database if not already done
def initialize_db():
    conn = sqlite3.connect(DATABASE)
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
    conn.commit()
    conn.close()

# Function to handle incoming post and queue processing
def handle_incoming_post(post_id, post_content, post_attachment):
    try:
        # Step 1: Store data in SQLite
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        # Check if the post already exists based on post_id
        cursor.execute("SELECT COUNT(*) FROM posts WHERE post_id = ?", (post_id,))
        if cursor.fetchone()[0] > 0:
            conn.close()
            return {"message": "Post already exists in the database"}

        # Insert the new post if it doesn't exist
        cursor.execute("""
            INSERT INTO posts (post_id, post_content, post_attachment, processed)
            VALUES (?, ?, ?, 0)
        """, (post_id, post_content, post_attachment))
        conn.commit()
        conn.close()

        # Step 2: Add post_id to Celery task queue
        process_post_task.delay(post_id)

        return {"message": "Post received and queued for processing"}

    except Exception as e:
        return {"error": str(e)}

# Step 3: Define Celery task to process the post
@celery_app.task(bind=True, max_retries=0)
def process_post_task(self, post_id):
    try:
        # Simulate processing
        print(f"Processing post with ID: {post_id}")

        # Update SQLite database to mark the post as processed
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE posts
            SET processed = 1
            WHERE post_id = ?
        """, (post_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        raise self.retry(exc=e)

# Initialize the database (run this once)
initialize_db()

# Define the API route to receive the post details
@app.route('/submit_post', methods=['POST'])
def submit_post():
    try:
        # Get JSON data from the POST request
        data = request.get_json()

        # Extract post details
        post_id = data.get('post_id')
        post_content = data.get('post_content')
        post_attachment = data.get('post_attachment')

        # Validate that all required fields are provided
        if not post_id or not post_content or not post_attachment:
            return jsonify({"error": "Missing required fields"}), 400

        # Handle incoming post and queue processing
        response = handle_incoming_post(post_id, post_content, post_attachment)
        return jsonify(response)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# DELETE API endpoint
@app.route('/delete_post/<post_id>', methods=['DELETE'])
def delete_post_api(post_id):
    try:
        delete_post(post_id)  # Call the delete_post function
        return jsonify({"message": f"Post with ID {post_id} deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to delete post {post_id}"}), 500


# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)

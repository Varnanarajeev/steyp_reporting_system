import time
import logging
import requests
import json
import re
import sqlite3
from typing import List
from together import Together

class ImageProcessor:
    def __init__(self):
        """Initialize the ImageProcessor with Together AI API."""
        self.api_key = "5d0190205dcf1b5fd7ec4f2e1c74a950cbc3d10d47d3466b9d4844d63297a099"
        self.client = Together(api_key=self.api_key)
        self.logger = self.get_logger()

    def get_logger(self):
        """Setup a basic logger."""
        logger = logging.getLogger("ImageProcessor")
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def fetch_unprocessed_images(self):
        """Fetch unprocessed post_id and image URLs from the database."""
        conn = sqlite3.connect("posts.db")
        cursor = conn.cursor()
        cursor.execute("SELECT post_id, post_attachment FROM posts WHERE processed = 0")
        data = cursor.fetchall()  # Fetch all unprocessed posts
        conn.close()
        return data if data else []

    def mark_post_as_processed(self, post_id: int):
        """Mark a post as processed in the database."""
        conn = sqlite3.connect("posts.db")
        cursor = conn.cursor()
        cursor.execute("UPDATE posts SET processed = 1 WHERE post_id = ?", (post_id,))
        conn.commit()
        conn.close()

    def analyze_images(self, prompt: str, images: List[tuple]) -> str:
        """Analyze images and send results to the backend API."""
        RETRY_DELAY = 10
        MAX_RETRIES = 3
        analysis_results = []
        processed_posts = set()  # Store processed post_ids to avoid duplicates

        for post_id, img_url in images:
            if post_id in processed_posts:
                self.logger.warning(f"Skipping duplicate post_id: {post_id}")
                continue  # Avoid reprocessing the same post

            attempt = 0
            while attempt < MAX_RETRIES:
                try:
                    self.logger.info(f"🔍 Analyzing image (Attempt {attempt + 1}): {img_url}")
                    response = self.client.chat.completions.create(
                        model="meta-llama/Llama-Vision-Free",
                        messages=[
                            {"role": "user",
                             "content": [
                                 {"type": "text", "text": prompt},
                                 {"type": "image_url", "image_url": {"url": img_url}}
                             ]}
                        ],
                        max_tokens=256,
                        temperature=0.5,
                        top_p=0.7,
                        top_k=50,
                        repetition_penalty=1,
                        stop=["<|eot_id|>", "<|eom_id|>"],
                        stream=False,
                    )

                    if response and hasattr(response, "choices") and response.choices:
                        analysis_text = response.choices[0].message.content.strip()
                        self.logger.info(f"AI Response: {analysis_text[:100]}...")

                        if analysis_text:
                            analysis_data = self.parse_analysis_text(analysis_text, post_id)
                            analysis_results.append(analysis_data)
                            # self.send_to_backend(analysis_data)

                            # Mark the post as processed
                            self.mark_post_as_processed(post_id)
                            processed_posts.add(post_id)  # Track processed post_id
                            break  # Move to the next image
                        else:
                            self.logger.warning(f"⚠️ Empty response for image: {img_url}")
                    else:
                        self.logger.warning(f"⚠️ No valid response for image: {img_url}")

                except Exception as e:
                    attempt += 1
                    if attempt < MAX_RETRIES:
                        self.logger.error(f"❌ Error: {str(e)}\nRetrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                    else:
                        self.logger.critical(f"🚨 Max retries reached. Skipping image: {img_url}")

        output_json = json.dumps(analysis_results[0], indent=4) if analysis_results else "{}"


        # Validate JSON output
        try:
            json.loads(output_json)
            self.logger.info("✅ Output is a valid JSON format")
        except json.JSONDecodeError:
            self.logger.error("❌ Output is NOT valid JSON format")

        return output_json

    def parse_analysis_text(self, text: str, post_id: str) -> dict:
        """Extracts key values from the AI response text and determines a toxicity score."""

        # Define keywords associated with different harmful content categories
        harmful_keywords = {
            "violence": ["attack", "knife", "gun", "murder", "assault", "harm", "blood"],
            "nudity": ["nude", "explicit", "sexual", "porn", "naked", "intimate"],
            "war": ["battle", "bomb", "explosion", "military", "warfare"],
            "discrimination": ["hate speech", "racist", "sexist", "discriminatory"]
        }

        # Initialize scores
        toxicity_score = 0

        # Check for each category in the AI-generated response text
        for category, keywords in harmful_keywords.items():
            for keyword in keywords:
                if re.search(rf"\b{keyword}\b", text, re.IGNORECASE):  # Case-insensitive word match
                    toxicity_score += 25  # Increase score for each detected category

        # Ensure toxicity score doesn't exceed 100
        toxicity_score = min(toxicity_score, 100)

        # Determine if the post should be removed
        is_remove = toxicity_score > 70

        # Generate appropriate summary
        if toxicity_score == 0:
            summary = "The Post does not contain any harmful content."
        elif is_remove:
            summary = "The Post contains harmful content and should be removed."
        else:
            summary = "The Post contains mild risks but is generally safe."

        return {
            "post_id": post_id,
            "is_remove": is_remove,
            "summary": summary,
            "toxicity_score": toxicity_score
        }

# Example Usage
if __name__ == "__main__":
    processor = ImageProcessor()

    prompt_text = """Analyze the given AI response and assess the following:

        - Toxicity Score (0-100): Rate the toxicity level of the image.
        - Nudity: Assess the presence of explanation aboout nudity or explicit sexual content.
        - Violence: Evaluate the level of violence or harm explained in the AI response.
        - War: Check for any explanation related to war, including conflict, military action, and destruction.
        - Discrimination: Identify any signs of discrimination, hate speech, or bias in the AI response.

    Provide the response in the following JSON format:

    {
        "post_id": "<actual_post_id>",
        "is_remove": <true/false>,
        "summary": "<brief summary of content safety, including specific assessments for nudity, violence, war, and discrimination>",
        "toxicity_score": <numeric_value>
    }
    """


    # Fetch only unprocessed images from the database
    unprocessed_images = processor.fetch_unprocessed_images()

    if unprocessed_images:
        output_json = processor.analyze_images(prompt_text, unprocessed_images)
        print(output_json)  # Print the output as JSON
    else:
        print("✅ No unprocessed images found in the database.")

import time
import logging
import json
import re
import sqlite3
from typing import List, Optional, Dict, Any
from together import Together
from pydantic import BaseModel, Field, field_validator

class AnalysisResult(BaseModel):
    post_id: str
    is_remove: bool
    summary: str
    toxicity_score: float = Field(ge=0, le=100)
    
    @field_validator('toxicity_score')
    @classmethod
    def validate_toxicity_score(cls, v):
        return min(max(v, 0), 100)  # Ensure toxicity score is between 0 and 100

class ImageProcessor:
    def __init__(self):
        """Initialize the ImageProcessor with Together AI API."""
        self.api_key = "cda097b3aa2b9f1668a83f55aa9f9d9664853b09d13cc23aee5c301cab5fa159"  # Store in environment variable for security
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
        """Fetch unprocessed posts and their attachments from the database."""
        conn = sqlite3.connect("posts.db")
        cursor = conn.cursor()
        cursor.execute("SELECT post_id, post_content, post_attachment FROM posts WHERE processed = 0")
        data = cursor.fetchall()
        conn.close()

        all_images = []
        for post_id, caption, attachments in data:
            image_list = json.loads(attachments)
            for img_url in image_list:
                all_images.append((post_id, caption, img_url))
        return all_images

    def generate_post_summary(self, caption: str, img_url: str) -> str:
        """Generate a 50-word explanation of the post, considering both caption and attachment."""
        prompt = f"""
        Generate a concise 50-word summary of the post's content. Consider the caption: "{caption}" and the attached image at {img_url}. Focus on the main idea and key details.
        """
        try:
            response = self.client.chat.completions.create(
                model="meta-llama/Llama-Vision-Free",
                messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
                max_tokens=75,
                temperature=0.7,
            )
            return response.choices[0].message.content.strip() if response and response.choices else ""
        except Exception as e:
            self.logger.error(f"Error generating summary: {e}")
            return ""

    def extract_json_from_text(self, text: str, post_id: str) -> Dict[str, Any]:
        """
        Try to extract JSON from text or create a structured JSON from the text analysis.
        Returns a dictionary with the expected format.
        """
        # First, look for complete JSON pattern in the text
        json_pattern = r'\{\s*"post_id"\s*:.*?\}'
        json_match = re.search(json_pattern, text, re.DOTALL)
        
        if json_match:
            try:
                json_data = json.loads(json_match.group(0))
                return json_data
            except json.JSONDecodeError:
                self.logger.error("Found JSON-like text but couldn't parse it")
        
        # Second, look for JSON without post_id
        json_pattern_no_id = r'\{\s*"is_remove"\s*:.*?\}'
        json_match_no_id = re.search(json_pattern_no_id, text, re.DOTALL)
        
        if json_match_no_id:
            try:
                json_data = json.loads(json_match_no_id.group(0))
                # Add the post_id that was missing
                json_data["post_id"] = post_id
                return json_data
            except json.JSONDecodeError:
                self.logger.error("Found partial JSON but couldn't parse it")
        
        # Third approach: extract key information from the text
        is_remove = False
        toxicity_score = 0
        summary = "The post is generally safe."
        
        # Check for explicit statements about removal recommendation
        remove_patterns = [
            r"is_remove\"?\s*:\s*true",  # JSON-like pattern
            r"remove\s*[=:]\s*true",  # Assignment-like pattern
            r"recommend(?:ed|ing)?\s*(?:to)?\s*remove",  # Natural language
            r"flag(?:ged|ging)?\s*(?:it|the post)?\s*for\s*removal",  # Flagging statements
            r"should be removed",  # Direct recommendation
            r"require(?:s|d)?\s*removal"  # Requirement statement
        ]
        
        for pattern in remove_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                is_remove = True
                summary = "The post contains harmful content or is irrelevant and should be removed."
                break
        
        # Check for statements indicating content is safe
        safe_patterns = [
            r"is_remove\"?\s*:\s*false",  # JSON-like pattern
            r"remove\s*[=:]\s*false",  # Assignment-like pattern
            r"does not (?:require|need) removal",  # Natural language negative
            r"should not be removed",  # Direct recommendation against removal
            r"(?:safe|appropriate|relevant|educational) content"  # Content quality indicators
        ]
        
        for pattern in safe_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                is_remove = False
                summary = "The post is relevant to educational purposes and contains no harmful content."
                break
        
        # Extract toxicity score if mentioned
        score_patterns = [
            r"toxicity[_\s]score\"?\s*:\s*(\d+)",  # JSON-like syntax
            r"toxicity[_\s]score\s*(?:is|=|:)\s*(\d+)",  # Assignment syntax
            r"toxicity[_\s]score\s*(?:of)?\s*(\d+)"  # Natural language
        ]
        
        for pattern in score_patterns:
            score_match = re.search(pattern, text, re.IGNORECASE)
            if score_match:
                try:
                    extracted_score = int(score_match.group(1))
                    if 0 <= extracted_score <= 100:
                        toxicity_score = extracted_score
                        break
                except (ValueError, IndexError):
                    pass
        
        # Extract summary if explicitly provided
        summary_patterns = [
            r"summary\"?\s*:\s*\"([^\"]+)\"",  # JSON string format
            r"summary\"?\s*:\s*'([^']+)'",     # JSON with single quotes
            r"summary\s*(?:is|=|:)\s*(.+?)(?:\.|$)" # Assignment with period termination
        ] 
        
        for pattern in summary_patterns:
            summary_match = re.search(pattern, text, re.IGNORECASE)
            if summary_match:
                extracted_summary = summary_match.group(1).strip()
                if extracted_summary:
                    summary = extracted_summary
                    break
        
        # Check for relevance statements if summary is not specific
        if "generally safe" in summary and "does not align" in text.lower():
            summary = "The post does not align with Steyp's educational and technological purpose."
            is_remove = True
        
        # Final relevance check override
        relevance_check = re.search(r"relevance check.{1,100}not fall under", text, re.IGNORECASE | re.DOTALL)
        if relevance_check and not is_remove:
            is_remove = True
            
        # Create the final result
        return {
            "post_id": post_id,
            "is_remove": is_remove,
            "summary": summary,
            "toxicity_score": toxicity_score
        }

    def analyze_images(self, prompt: str, images: List[tuple]) -> str:
        """Analyze images and send results to the backend API."""
        RETRY_DELAY = 5
        MAX_RETRIES = 3
        analysis_results = {}

        for post_id, caption, img_url in images:
            attempt = 0
            while attempt < MAX_RETRIES:
                try:
                    post_summary = self.generate_post_summary(caption, img_url)
                    modified_prompt = f"""
                {prompt}\n\nPost Summary: {post_summary}

                You are a JSON extractor. Return a structured JSON in this exact format:
                {{
                    "post_id": "{post_id}",
                    "is_remove": <true/false>,
                    "summary": "<brief summary>",
                    "toxicity_score": <numeric_value between 0-100>
                }}
                Ensure that "toxicity_score" is a valid integer between 0-100.
                """

                    
                    self.logger.info(f"Analyzing post {post_id} (Attempt {attempt + 1})")
                    response = self.client.chat.completions.create(
                        model="meta-llama/Llama-Vision-Free",
                        messages=[{"role": "user", "content": [
                            {"type": "text", "text": modified_prompt},
                            {"type": "image_url", "image_url": {"url": img_url}}
                        ]}],
                        max_tokens=256,
                        temperature=0.7,
                    )
                    
                    if response and response.choices:
                        analysis_text = response.choices[0].message.content.strip()
                        self.logger.info(f"AI Response: {analysis_text[:1000]}...")

                        try:
                            # First attempt to parse the response as JSON directly
                            json_data = json.loads(analysis_text)
                            # Add post_id if missing
                            if "post_id" not in json_data:
                                json_data["post_id"] = post_id
                            analysis_data = AnalysisResult.model_validate(json_data)
                            
                        except (json.JSONDecodeError, ValueError):
                            # If direct JSON parsing fails, use our extraction method
                            self.logger.info("Direct JSON parsing failed, using extraction method")
                            json_data = self.extract_json_from_text(analysis_text, post_id)
                            analysis_data = AnalysisResult.model_validate(json_data)
                        
                        # Log the extracted data for debugging
                        self.logger.info(f"Extracted data: {json_data}")
                        
                        # Store the validated data
                        if post_id not in analysis_results:
                            analysis_results[post_id] = {"toxicity_scores": [], "is_remove": False, "summary": ""}
                        
                        analysis_results[post_id]["toxicity_scores"].append(analysis_data.toxicity_score)
                        if analysis_data.is_remove:
                            analysis_results[post_id]["is_remove"] = True
                            analysis_results[post_id]["summary"] = analysis_data.summary
                        
                        self.mark_post_as_processed(post_id)
                        break
                    else:
                        self.logger.warning(f"No valid response for image: {img_url}")

                except Exception as e:
                    self.logger.error(f"Error during processing: {str(e)}")
                    attempt += 1
                    if attempt < MAX_RETRIES:
                        self.logger.error(f"Retrying in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                    else:
                        self.logger.critical(f"Max retries reached. Skipping image: {img_url}")

        final_results = []
        for post_id, data in analysis_results.items():
            max_toxicity = max(data["toxicity_scores"]) if data["toxicity_scores"] else 0
            is_remove = data["is_remove"]
            summary = data.get("summary") or ("The post contains harmful content and should be removed." if is_remove else "The post is generally safe.")
            
            result = AnalysisResult(
                post_id=post_id,
                is_remove=is_remove,
                summary=summary,
                toxicity_score=max_toxicity
            )
            final_results.append(result.model_dump())

        return json.dumps(final_results, indent=4)

    def mark_post_as_processed(self, post_id):
        """Update the post's 'processed' column to 1."""
        try:
            conn = sqlite3.connect("posts.db")
            cursor = conn.cursor()
            cursor.execute("UPDATE posts SET processed = 1 WHERE post_id = ?", (post_id,))
            conn.commit()
            conn.close()
            self.logger.info(f"Marked post {post_id} as processed.")
        except Exception as e:
            self.logger.error(f"Failed to mark post {post_id} as processed: {e}")


# Example Usage
if __name__ == "__main__":
    processor = ImageProcessor()
    prompt_text = """ Platform Overview: Steyp is a digital university platform by Talrop that helps students become computer engineers and tech scientists. It offers structured programs for school and college students, providing personalized mentoring, real-time progress tracking, and industry-ready knowledge.

Task:

Analyze the given post (image and text) which is given through a 50-word explanation summarizing its content. Use this explanation to determine whether the post aligns with Steyp's educational and technological purpose. If the content is irrelevant, inappropriate, or non-educational, flag it for removal.

Evaluation Steps:

1. Check Educational Relevance:
   - A post is relevant if it falls under at least one of the following categories:
     - Computer Science & Programming
     - Emerging Technologies (AI, IoT, Blockchain, etc.)
     - Engineering & Innovation
     - Educational Career Guidance
     - Ethical & Social Aspects of Technology
     - Industry Trends & Scientific Research
     - Mathematics & Logical Thinking
     - Cybersecurity & Data Privacy
     - Robotics & Automation
     - Software Development & IT Infrastructure
     - Networking & Communication Technologies
     - Cloud Computing & DevOps
     - Entrepreneurship & Startups in Tech
     - Physics, Electronics & Digital Systems
     - Technical Writing & Research in STEM
     - Open Source Contributions & Development

   - Removal Condition: If the image or text does not align with the above categories, flag it for removal.

2. Assess Content Safety:
   - Each post is evaluated based on the following content safety criteria:
     - Toxicity Score (0-100): Assess the level of harmful or inappropriate elements. Content that is highly irrelevant to the educational goals of the platform should receive a higher toxicity score (e.g., 75 or above).
     - Nudity: Flag explicit, sexual, or suggestive content.
     - Violence: Detect aggressive, threatening, or intimidating content.
     - War/Conflict: Identify distressing war-related themes (unless educational, e.g., cybersecurity in warfare).
     - Discrimination: Flag hate speech, bias, or discriminatory remarks.
     - Drug Consumption: Detect references to drug use.
     - Profanity: Identify offensive language inappropriate for a professional learning environment.
     - Caption-Content Mismatch: Ensure the caption aligns with the image and does not misrepresent it.
     - Content-Attachment Mismatch: Verify that the text aligns with the attached media.

   - Removal Condition: If a post contains explicit content, extreme violence, discrimination, or excessive profanity, flag it for removal.

3. Handling of Edge Cases:
   - Gray Areas: If a post contains both educational and mildly inappropriate content, flag it for manual review instead of immediate removal.
   - Historical References: Posts discussing historical wars in an educational context (e.g., WWII's impact on computing) are acceptable.
   - Repeated Content Analysis: If the same post is analyzed again, maintain a consistent score, but allow contextual overrides if needed.

Response Format:

You are a json text extractor. return the following json:

{
    "post_id": "<actual_post_id>",
    "is_remove": <true/false>,
    "summary": "<brief summary of content safety, including specific assessments for nudity, violence, war, and discrimination>",
    "toxicity_score": <numeric_value>
}

Additional Notes:

- Relevance and toxicity scoring should be handled separately. A post can be irrelevant but non-toxic, or toxic but relevant.
- Threshold for manual review: If a post scores between 50-70 on toxicity but has educational value, flag it for manual review instead of automatic removal.
- Human Moderation Escalation: Posts with severe violations (toxicity > 85, explicit content, hate speech) should be immediately removed without manual review.
"""
    unprocessed_images = processor.fetch_unprocessed_images()
    if unprocessed_images:
        output_json = processor.analyze_images(prompt_text, unprocessed_images)
        print(output_json)
    else:
        print("No unprocessed images found in the database.")
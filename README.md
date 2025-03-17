# AI-Based Post Reporting System
The objective of this system is to automatically assess and report harmful content in posts
 published on a social media platform. The content will be evaluated by an AI service, using
 generative models (LLMs), to detect harmful elements, including offensive language,
 inappropriate topics, or any content violating the platform's community guidelines.
 The process will involve scheduling posts for reporting, running assessments, and removing
 harmful content based on AI-generated results. If the AI model is uncertain or below the
 threshold of confidence (70%), the post will be flagged for manual review.Its just a prototype which need to be 
 implemented into the backend of an already existing application.


## Features
- **Automated Content Moderation**: Detects offensive language and inappropriate images.
- **Confidence-Based Reporting**: Automatically removes posts with high confidence and flags uncertain cases for review.
- **Task Scheduling**: Processes posts asynchronously using Celery.
- **Manual Review System**: Moderators can review and override AI decisions.

## Tech Stack
- **Backend**: Python
- **AI Models**: Mistral (Text), Meta Vision (Images)
- **Task Scheduling**: Celery
- **Database**: sqlite



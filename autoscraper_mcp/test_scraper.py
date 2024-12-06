import asyncio
import logging
from pathlib import Path
from autoscraper_mcp.server import AutoScraperServer
from playwright.async_api import async_playwright

# Configure logging
log_dir = Path('/Users/wbiaz/claude/logs/autoscraper-mcp')
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_dir / 'test.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_scraping():
    server = AutoScraperServer()
    
    # Test URL
    url = 'https://www.yabiladi.com/'
    
    # Example data to train on
    wanted_data = [
        "Les 10 articles les plus lus",  # Section title for popular articles 
    ]
    
    try:
        # Initialize scraper
        init_result = await server.call_tool({
            'tool_name': 'init_scraper',
            'tool_input': {
                'url': url,
                'wanted_data': wanted_data,
                'screenshot': True
            }
        })
        
        logger.info(f"Init result: {init_result}")
        
        # Scrape data
        scrape_result = await server.call_tool({
            'tool_name': 'scrape_data',
            'tool_input': {
                'url': url,
                'storage': {
                    'type': 'sqlite',
                    'path': str(log_dir / 'results.db'),
                    'table_name': 'articles'
                }
            }
        })
        
        logger.info(f"Scrape result: {scrape_result}")
        
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        raise

if __name__ == '__main__':
    asyncio.run(test_scraping())
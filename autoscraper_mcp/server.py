import os
import logging
import json
from pathlib import Path
from typing import Dict, List, Optional, Any

from anthropic_mcp import (
    Server,
    Tool,
    ListToolsRequestSchema,
    ListToolsResponseSchema,
    ToolCallRequestSchema,
    ToolCallResponseSchema
)
from autoscraper import AutoScraper
from playwright.async_api import async_playwright

# Configure logging
log_dir = Path('/Users/wbiaz/claude/logs/autoscraper-mcp')
log_dir.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=log_dir / 'autoscraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class AutoScraperServer(Server):
    def __init__(self):
        super().__init__()
        self.scraper = AutoScraper()
        self.storage_backends = {}
        self.current_browser = None
        logger.info('AutoScraperServer initialized')

    async def list_tools(self, request: ListToolsRequestSchema) -> ListToolsResponseSchema:
        """List available tools for AutoScraper MCP server."""
        return ListToolsResponseSchema(tools=[
            Tool(
                name='init_scraper',
                description='Initialize and train AutoScraper with examples',
                inputSchema={
                    'type': 'object',
                    'properties': {
                        'url': {'type': 'string', 'description': 'Target webpage URL'},
                        'wanted_data': {'type': 'array', 'description': 'Example data to train scraper with'},
                        'screenshot': {'type': 'boolean', 'description': 'Take screenshot of page'}
                    },
                    'required': ['url', 'wanted_data']
                }
            ),
            Tool(
                name='scrape_data',
                description='Scrape data using trained model',
                inputSchema={
                    'type': 'object', 
                    'properties': {
                        'url': {'type': 'string'},
                        'storage_type': {'type': 'string', 'enum': ['sqlite', 'json', 'csv']},
                        'storage_path': {'type': 'string'}
                    },
                    'required': ['url']
                }
            )
        ])

    async def call_tool(self, request: ToolCallRequestSchema) -> ToolCallResponseSchema:
        """Handle tool calls for AutoScraper operations."""
        try:
            if request.tool_name == 'init_scraper':
                result = await self._init_scraper(request.tool_input)
            elif request.tool_name == 'scrape_data':
                result = await self._scrape_data(request.tool_input)
            else:
                raise ValueError(f'Unknown tool: {request.tool_name}')
            
            return ToolCallResponseSchema(tool_output=result)
        except Exception as e:
            logger.error(f'Error in tool {request.tool_name}: {str(e)}')
            raise

    async def _init_scraper(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Initialize and train the AutoScraper."""
        url = input_data['url']
        wanted_data = input_data['wanted_data']
        take_screenshot = input_data.get('screenshot', False)

        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url)

            if take_screenshot:
                screenshot_path = log_dir / f'screenshot_{hash(url)}.png'
                await page.screenshot(path=str(screenshot_path))

            html_content = await page.content()
            await browser.close()

        result = self.scraper.build(html=html_content, wanted_list=wanted_data)
        return {
            'training_result': result,
            'screenshot_path': str(screenshot_path) if take_screenshot else None
        }

    async def _scrape_data(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute scraping using trained model."""
        url = input_data['url']
        storage_type = input_data.get('storage_type', 'json')
        storage_path = input_data.get('storage_path')

        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url)
            html_content = await page.content()
            await browser.close()

        result = self.scraper.get_result_similar(html=html_content)

        if storage_path:
            if storage_type == 'sqlite':
                # TODO: Implement SQLite storage
                pass
            elif storage_type == 'csv':
                # TODO: Implement CSV storage
                pass
            elif storage_type == 'json':
                with open(storage_path, 'w') as f:
                    json.dump(result, f)

        return {
            'scraped_data': result,
            'storage_path': storage_path
        }

def main():
    """Entry point for the AutoScraper MCP server."""
    server = AutoScraperServer()
    server.run()

if __name__ == '__main__':
    main()

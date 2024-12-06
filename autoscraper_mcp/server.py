import os
import logging
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

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

from .storage import SQLiteStorage, CSVStorage, JSONStorage

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
    """MCP server for AutoScraper with integrated storage support."""

    def __init__(self):
        super().__init__()
        self.scraper = AutoScraper()
        self.storage_backends = {
            'sqlite': SQLiteStorage(),
            'csv': CSVStorage(),
            'json': JSONStorage()
        }
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
                        'wanted_data': {
                            'type': 'array',
                            'description': 'Example data to train scraper with',
                            'items': {'type': 'string'}
                        },
                        'screenshot': {
                            'type': 'boolean',
                            'description': 'Take screenshot of page',
                            'default': False
                        }
                    },
                    'required': ['url', 'wanted_data']
                }
            ),
            Tool(
                name='scrape_data',
                description='Scrape data using trained model and store results',
                inputSchema={
                    'type': 'object',
                    'properties': {
                        'url': {'type': 'string', 'description': 'Target webpage URL'},
                        'storage': {
                            'type': 'object',
                            'properties': {
                                'type': {
                                    'type': 'string',
                                    'enum': ['sqlite', 'csv', 'json'],
                                    'description': 'Storage backend type'
                                },
                                'path': {
                                    'type': 'string',
                                    'description': 'Path to store the data'
                                },
                                'table_name': {
                                    'type': 'string',
                                    'description': 'Table name for SQLite storage',
                                    'default': 'scraped_data'
                                },
                                'encoding': {
                                    'type': 'string',
                                    'description': 'File encoding for CSV/JSON',
                                    'default': 'utf-8'
                                },
                                'append': {
                                    'type': 'boolean',
                                    'description': 'Append to existing data',
                                    'default': False
                                }
                            },
                            'required': ['type', 'path']
                        }
                    },
                    'required': ['url', 'storage']
                }
            ),
            Tool(
                name='save_scraper',
                description='Save trained scraper model',
                inputSchema={
                    'type': 'object',
                    'properties': {
                        'path': {'type': 'string', 'description': 'Path to save model'}
                    },
                    'required': ['path']
                }
            ),
            Tool(
                name='load_scraper',
                description='Load trained scraper model',
                inputSchema={
                    'type': 'object',
                    'properties': {
                        'path': {'type': 'string', 'description': 'Path to load model from'}
                    },
                    'required': ['path']
                }
            )
        ])

    async def _get_page_content(self, url: str, screenshot: bool = False) -> tuple[str, Optional[str]]:
        """Get page content using Playwright.
        
        Args:
            url: Target webpage URL
            screenshot: Whether to take a screenshot
            
        Returns:
            Tuple of (html_content, screenshot_path)
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url)

            screenshot_path = None
            if screenshot:
                screenshot_path = str(log_dir / f'screenshot_{hash(url)}.png')
                await page.screenshot(path=screenshot_path)

            html_content = await page.content()
            await browser.close()
            
            return html_content, screenshot_path

    async def _store_data(self, data: List[Dict], storage_config: Dict) -> Dict[str, Any]:
        """Store scraped data using specified backend.
        
        Args:
            data: Scraped data to store
            storage_config: Storage configuration
            
        Returns:
            Dictionary with storage results
        """
        storage_type = storage_config['type']
        if storage_type not in self.storage_backends:
            raise ValueError(f'Unsupported storage type: {storage_type}')

        backend = self.storage_backends[storage_type]
        path = storage_config['path']
        
        if storage_config.get('append', False):
            await backend.append(data, path, **storage_config)
        else:
            await backend.save(data, path, **storage_config)

        return {
            'storage_type': storage_type,
            'storage_path': path,
            'record_count': len(data)
        }

    async def call_tool(self, request: ToolCallRequestSchema) -> ToolCallResponseSchema:
        """Handle tool calls for AutoScraper operations."""
        try:
            if request.tool_name == 'init_scraper':
                url = request.tool_input['url']
                wanted_data = request.tool_input['wanted_data']
                screenshot = request.tool_input.get('screenshot', False)

                html_content, screenshot_path = await self._get_page_content(url, screenshot)
                result = self.scraper.build(html=html_content, wanted_list=wanted_data)

                return ToolCallResponseSchema(tool_output={
                    'training_result': result,
                    'screenshot_path': screenshot_path
                })

            elif request.tool_name == 'scrape_data':
                url = request.tool_input['url']
                storage_config = request.tool_input['storage']

                html_content, _ = await self._get_page_content(url)
                scraped_data = self.scraper.get_result_similar(html=html_content)
                
                if not isinstance(scraped_data, list):
                    scraped_data = [scraped_data]
                
                # Convert to list of dicts if necessary
                if scraped_data and not isinstance(scraped_data[0], dict):
                    scraped_data = [{'value': item} for item in scraped_data]

                storage_result = await self._store_data(scraped_data, storage_config)

                return ToolCallResponseSchema(tool_output={
                    'scraped_data': scraped_data[:5],  # Preview first 5 items
                    'total_items': len(scraped_data),
                    'storage': storage_result
                })

            elif request.tool_name == 'save_scraper':
                path = request.tool_input['path']
                self.scraper.save(path)
                return ToolCallResponseSchema(tool_output={
                    'message': f'Scraper model saved to {path}'
                })

            elif request.tool_name == 'load_scraper':
                path = request.tool_input['path']
                self.scraper.load(path)
                return ToolCallResponseSchema(tool_output={
                    'message': f'Scraper model loaded from {path}'
                })

            else:
                raise ValueError(f'Unknown tool: {request.tool_name}')

        except Exception as e:
            logger.error(f'Error in tool {request.tool_name}: {str(e)}')
            raise

def main():
    """Entry point for the AutoScraper MCP server."""
    server = AutoScraperServer()
    server.run()

if __name__ == '__main__':
    main()
# AutoScraper MCP Server

This is a Model Context Protocol (MCP) server implementation for AutoScraper, providing seamless integration with browser automation and flexible storage options.

## Features

- Browser automation integration (Playwright)
- Multiple storage backends (SQLite, CSV, JSON)
- Screenshot capabilities
- Logging and error tracking
- Flexible data extraction

## Installation

```bash
pip install .
```

## Usage

```bash
autoscraper-mcp
```

## Configuration

Add to your Claude Desktop configuration:

```json
{
    "type": "stdio",
    "command": "autoscraper-mcp"
}
```

## Tools

### init_scraper
Initialize and train AutoScraper with examples:
- url: Target webpage URL
- wanted_data: Example data to train scraper with
- screenshot: Take screenshot of page (optional)

### scrape_data
Scrape data using trained model:
- url: Target webpage URL
- storage_type: Type of storage (sqlite, json, csv)
- storage_path: Path to save scraped data

## License

MIT

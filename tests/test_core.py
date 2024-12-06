import pytest
import asyncio
from pathlib import Path
import tempfile

from autoscraper_mcp.server import AutoScraperServer
from autoscraper_mcp.storage import SQLiteStorage, CSVStorage, JSONStorage

@pytest.fixture
def server():
    return AutoScraperServer()

@pytest.fixture
def test_data():
    return [
        {"title": "Test Event 1", "date": "2024-01-01", "price": 100},
        {"title": "Test Event 2", "date": "2024-01-02", "price": 200}
    ]

@pytest.mark.asyncio
async def test_init_scraper(server):
    """Test basic scraper initialization"""
    request = {
        'url': 'https://example.com',
        'wanted_data': ['Example Data'],
        'screenshot': True
    }
    
    result = await server.call_tool({
        'tool_name': 'init_scraper',
        'tool_input': request
    })
    
    assert 'training_result' in result.tool_output
    if result.tool_output.get('screenshot_path'):
        assert Path(result.tool_output['screenshot_path']).exists()

@pytest.mark.asyncio
async def test_basic_storage_operations(server, test_data):
    """Test storage operations with each backend"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test each storage backend
        storage_configs = [
            {'type': 'sqlite', 'path': f'{tmpdir}/test.db'},
            {'type': 'csv', 'path': f'{tmpdir}/test.csv'},
            {'type': 'json', 'path': f'{tmpdir}/test.json'}
        ]
        
        for config in storage_configs:
            request = {
                'url': 'https://example.com',
                'storage': config
            }
            
            # Mock scraping result
            server.scraper.get_result_similar = lambda **kwargs: test_data
            
            result = await server.call_tool({
                'tool_name': 'scrape_data',
                'tool_input': request
            })
            
            assert result.tool_output['storage']['record_count'] == len(test_data)
            assert Path(config['path']).exists()

@pytest.mark.asyncio
async def test_error_handling(server):
    """Test basic error handling"""
    # Test invalid tool
    with pytest.raises(ValueError):
        await server.call_tool({
            'tool_name': 'invalid_tool',
            'tool_input': {}
        })
    
    # Test invalid storage type
    with pytest.raises(ValueError):
        await server.call_tool({
            'tool_name': 'scrape_data',
            'tool_input': {
                'url': 'https://example.com',
                'storage': {'type': 'invalid', 'path': 'test.txt'}
            }
        })

@pytest.mark.asyncio
async def test_model_save_load(server):
    """Test model saving and loading"""
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = f'{tmpdir}/model.pkl'
        
        # Train model first
        await server.call_tool({
            'tool_name': 'init_scraper',
            'tool_input': {
                'url': 'https://example.com',
                'wanted_data': ['Example Data']
            }
        })
        
        # Save model
        save_result = await server.call_tool({
            'tool_name': 'save_scraper',
            'tool_input': {'path': model_path}
        })
        assert Path(model_path).exists()
        
        # Load model
        load_result = await server.call_tool({
            'tool_name': 'load_scraper',
            'tool_input': {'path': model_path}
        })
        assert 'message' in load_result.tool_output
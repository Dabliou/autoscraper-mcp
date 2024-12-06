from setuptools import setup, find_packages

setup(
    name='autoscraper-mcp',
    version='0.1.0',
    description='MCP server for AutoScraper with browser automation integration',
    author='Your Name',
    packages=find_packages(),
    install_requires=[
        'anthropic-mcp',
        'autoscraper',
        'playwright',
        'sqlalchemy',
        'pandas',
        'aiohttp',
        'pyyaml'
    ],
    entry_points={
        'console_scripts': [
            'autoscraper-mcp=autoscraper_mcp.server:main',
        ],
    },
)
#!/usr/bin/env python3
"""
Async Web Server CLI Script
A simple CLI script that spawns an async web server that serves requests on specified routes.

Usage:
    python async_web_server.py --host=127.0.0.1 --port=8080 --routes=/a,/b,/c,/d,/e
"""

import argparse
import asyncio
from aiohttp import web
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def handle_request(request):
    """Generic handler for all routes."""
    route = request.path
    logger.info(f"Request received on route: {route}")
    return web.Response(text=f"Hello from {route}!")

def create_app(routes):
    """Create the web application with the specified routes."""
    app = web.Application()
    
    # Add routes to the application
    for route in routes:
        app.router.add_get(route, handle_request)
        logger.info(f"Route added: {route}")
    
    return app

async def start_server(host, port, routes):
    """Start the web server."""
    app = create_app(routes)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, host, port)
    
    logger.info(f"Starting server on http://{host}:{port}")
    await site.start()
    
    # Keep the server running
    try:
        while True:
            await asyncio.sleep(3600)  # Sleep for an hour (or any long duration)
    except asyncio.CancelledError:
        logger.info("Server shutdown initiated")
    finally:
        await runner.cleanup()
        logger.info("Server has been shut down")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Async Web Server with custom routes')
    
    parser.add_argument('--host', type=str, default='127.0.0.1',
                       help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=8080,
                       help='Port to bind the server to')
    parser.add_argument('--routes', type=str, default='/hello',
                       help='Comma-separated list of routes (e.g., /a,/b,/c)')
    
    args = parser.parse_args()
    
    # Parse routes from comma-separated string to list
    routes_list = args.routes.split(',')
    
    return args.host, args.port, routes_list

def main():
    """Main entry point of the script."""
    host, port, routes = parse_arguments()
    
    logger.info(f"Configuring server with:")
    logger.info(f"  Host: {host}")
    logger.info(f"  Port: {port}")
    logger.info(f"  Routes: {routes}")
    
    # Run the async server
    try:
        asyncio.run(start_server(host, port, routes))
    except KeyboardInterrupt:
        logger.info("Server stopped by user")

if __name__ == "__main__":
    main()
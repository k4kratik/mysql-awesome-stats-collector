"""CLI entry point for MySQL Awesome Stats Collector."""

import argparse
import sys
import os

from . import __version__


def main():
    """Main entry point for the masc CLI."""
    parser = argparse.ArgumentParser(
        prog="masc",
        description="MySQL Awesome Stats Collector - Collect and visualize MySQL diagnostics from multiple hosts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  masc                          # Start server on default port 8000
  masc --port 9000              # Start server on port 9000
  masc --host 0.0.0.0           # Listen on all interfaces
  masc --reload                 # Enable auto-reload for development

Environment Variables:
  MASC_HOSTS_FILE               # Path to custom hosts.yaml file
  MASC_RUNS_DIR                 # Directory for job outputs
  
For more information, visit: https://github.com/k4kratik/mysql-awesome-stats-collector
        """
    )
    
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the server to (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=8000,
        help="Port to run the server on (default: 8000)"
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development"
    )
    parser.add_argument(
        "--hosts-file",
        type=str,
        default=None,
        help="Path to hosts.yaml configuration file"
    )
    parser.add_argument(
        "--version", "-v",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    
    args = parser.parse_args()
    
    # Set hosts file override if provided
    if args.hosts_file:
        os.environ["MASC_HOSTS_FILE"] = args.hosts_file
    
    # Import uvicorn here to avoid slow startup for --help/--version
    try:
        import uvicorn
    except ImportError:
        print("Error: uvicorn is not installed. Install with: pip install uvicorn[standard]")
        sys.exit(1)
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║           MySQL Awesome Stats Collector (MASC)               ║
║          Collect & Visualize MySQL Diagnostics               ║
╚══════════════════════════════════════════════════════════════╝
  Version: {__version__}

  Starting server at http://{args.host}:{args.port}
  Press Ctrl+C to stop
    """)
    
    uvicorn.run(
        "app.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )


if __name__ == "__main__":
    main()


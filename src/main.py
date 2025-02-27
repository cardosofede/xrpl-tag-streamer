"""
Main application entry point.
Provides command line interface for running different components.
"""

import asyncio

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from src.config import TARGET_TAG, HISTORY_BACKFILL_DAYS
from src.db.database import XRPLDatabase
from src.streaming.historical import process_history, process_ledger_range
from src.streaming.streamer import run_streamer_sync
from src.utils.logger import setup_logging

# Set up logger
logger = setup_logging()

# Create Typer app
app = typer.Typer(help="XRPL Transaction Tag Streamer")
console = Console()

@app.command()
def stream(
    tag: str = typer.Option(TARGET_TAG, "--tag", "-t", help="Tag to filter transactions for"),
    history_days: int = typer.Option(
        HISTORY_BACKFILL_DAYS, 
        "--days", 
        "-d", 
        help="Number of days to backfill historical data (0 to disable)"
    ),
):
    """
    Stream transactions from XRPL and filter for a specific tag.
    Optionally process historical data first.
    """
    console.print(f"[bold green]Starting XRPL Tag Streamer[/bold green]")
    console.print(f"Filtering for tag: [bold cyan]{tag}[/bold cyan]")
    
    # Process historical data if enabled
    if history_days > 0:
        console.print(f"[bold yellow]Processing historical data for the past {history_days} days...[/bold yellow]")
        asyncio.run(process_history(days=history_days))
        console.print("[bold green]Historical data processing complete[/bold green]")
    
    # Start real-time streaming
    console.print("[bold yellow]Starting real-time transaction streaming...[/bold yellow]")
    console.print("Press CTRL+C to stop streaming")
    run_streamer_sync()

@app.command()
def history(
    tag: str = typer.Option(TARGET_TAG, "--tag", "-t", help="Tag to filter transactions for"),
    days: int = typer.Option(
        7, 
        "--days", 
        "-d", 
        help="Number of days to process (from now)"
    ),
    start_ledger: int = typer.Option(
        None, 
        "--start-ledger", 
        "-s", 
        help="Start ledger index (overrides days)"
    ),
    end_ledger: int = typer.Option(
        None, 
        "--end-ledger", 
        "-e", 
        help="End ledger index (required if start-ledger is specified)"
    ),
):
    """
    Process historical XRPL transactions and filter for a specific tag.
    Either specify a number of days to look back, or a specific ledger range.
    """
    console.print(f"[bold green]XRPL Historical Transaction Processor[/bold green]")
    console.print(f"Filtering for tag: [bold cyan]{tag}[/bold cyan]")
    
    if start_ledger is not None and end_ledger is not None:
        # Process specific ledger range
        console.print(f"[bold yellow]Processing ledgers {start_ledger} to {end_ledger}...[/bold yellow]")
        asyncio.run(process_ledger_range(start_ledger, end_ledger))
    else:
        # Process by days
        console.print(f"[bold yellow]Processing transactions for the past {days} days...[/bold yellow]")
        asyncio.run(process_history(days=days))
    
    console.print("[bold green]Historical data processing complete[/bold green]")

@app.command()
def query(
    tag: str = typer.Option(TARGET_TAG, "--tag", "-t", help="Tag to query transactions for"),
    limit: int = typer.Option(20, "--limit", "-l", help="Maximum number of transactions to return"),
    export: str = typer.Option(None, "--export", "-e", help="Export results to CSV file"),
):
    """
    Query stored transactions that match a specific tag.
    Optionally export results to a CSV file.
    """
    console.print(f"[bold green]XRPL Transaction Query[/bold green]")
    console.print(f"Querying for tag: [bold cyan]{tag}[/bold cyan]")
    
    # Query transactions from database
    db = XRPLDatabase()
    transactions = db.get_transactions_with_tag(tag, limit=limit)
    
    if transactions.empty:
        console.print("[bold yellow]No matching transactions found[/bold yellow]")
        return
    
    # Display results in a table
    console.print(f"[bold green]Found {len(transactions)} matching transactions:[/bold green]")
    
    # Create a rich table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Hash")
    table.add_column("Ledger")
    table.add_column("Type")
    table.add_column("Account")
    table.add_column("Destination")
    table.add_column("Amount")
    table.add_column("Time")
    
    # Add rows to the table
    for _, tx in transactions.iterrows():
        table.add_row(
            tx.get("tx_hash", "")[:10] + "...",
            str(tx.get("ledger_index", "")),
            tx.get("transaction_type", ""),
            tx.get("account", "")[:10] + "...",
            tx.get("destination", "")[:10] + "..." if tx.get("destination") else "",
            str(tx.get("amount", "")),
            tx.get("tx_time", "").strftime("%Y-%m-%d %H:%M:%S") if pd.notna(tx.get("tx_time")) else "",
        )
    
    console.print(table)
    
    # Export to CSV if requested
    if export:
        export_path = export if export.endswith(".csv") else f"{export}.csv"
        transactions.to_csv(export_path, index=False)
        console.print(f"[bold green]Exported results to {export_path}[/bold green]")

@app.command()
def stats():
    """
    Display statistics about stored transactions.
    """
    console.print(f"[bold green]XRPL Transaction Statistics[/bold green]")
    
    db = XRPLDatabase()
    
    # Get ledger stats
    latest_ledger = db.get_latest_ledger_index()
    ledger_count = db.con.execute("SELECT COUNT(*) FROM ledgers").fetchone()[0]
    
    # Get transaction stats
    tx_count = db.con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    matched_tx_count = db.con.execute("SELECT COUNT(*) FROM transactions WHERE tag_matched = true").fetchone()[0]
    
    # Create a rich table for stats
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Statistic")
    table.add_column("Value")
    
    table.add_row("Latest Ledger", str(latest_ledger))
    table.add_row("Ledgers Stored", str(ledger_count))
    table.add_row("Transactions Stored", str(tx_count))
    table.add_row("Matching Transactions", str(matched_tx_count))
    
    console.print(table)
    
    # Get daily transaction counts
    daily_counts = db.con.execute("""
        SELECT date_trunc('day', tx_time) as date, COUNT(*) as count
        FROM transactions
        GROUP BY date_trunc('day', tx_time)
        ORDER BY date DESC
        LIMIT 7
    """).fetchall()
    
    if daily_counts:
        console.print("\n[bold green]Daily Transaction Counts:[/bold green]")
        daily_table = Table(show_header=True, header_style="bold magenta")
        daily_table.add_column("Date")
        daily_table.add_column("Transaction Count")
        
        for date, count in daily_counts:
            daily_table.add_row(str(date), str(count))
        
        console.print(daily_table)

if __name__ == "__main__":
    app()
"""
Test Script for Bulk Peers Collection and Queries
Tests the BulkPeersCollector and peers_helpers functions
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.utils.peers_helpers import (
    get_peers,
    get_peers_raw,
    find_common_peers,
    are_peers,
    get_peer_network,
    get_peer_counts,
    find_most_connected,
    search_by_peer
)


def test_basic_queries():
    """Test basic peer queries"""
    print("\n" + "="*80)
    print("TEST 1: Basic Peer Queries")
    print("="*80)

    with get_session() as session:
        # Test symbols to query
        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN']

        for symbol in test_symbols:
            peers = get_peers(session, symbol)
            if peers:
                print(f"\n{symbol} has {len(peers)} peers:")
                print(f"  {', '.join(peers[:10])}")  # Show first 10
                if len(peers) > 10:
                    print(f"  ... and {len(peers) - 10} more")
            else:
                print(f"\n{symbol}: No peers found")


def test_common_peers():
    """Test finding common peers between symbols"""
    print("\n" + "="*80)
    print("TEST 2: Common Peers Between Symbols")
    print("="*80)

    with get_session() as session:
        # Test pairs
        test_pairs = [
            ('AAPL', 'MSFT'),
            ('GOOGL', 'META'),
            ('JPM', 'BAC'),
            ('TSLA', 'F')
        ]

        for symbol1, symbol2 in test_pairs:
            common = find_common_peers(session, symbol1, symbol2)
            if common:
                print(f"\n{symbol1} & {symbol2} have {len(common)} common peers:")
                print(f"  {', '.join(common[:5])}")  # Show first 5
            else:
                print(f"\n{symbol1} & {symbol2}: No common peers")

            # Check if they're mutual peers
            mutual = are_peers(session, symbol1, symbol2)
            print(f"  Are they mutual peers? {mutual}")


def test_peer_network():
    """Test peer network traversal"""
    print("\n" + "="*80)
    print("TEST 3: Peer Network (Multi-Level)")
    print("="*80)

    with get_session() as session:
        test_symbols = ['AAPL', 'JPM']

        for symbol in test_symbols:
            print(f"\nPeer network for {symbol}:")
            network = get_peer_network(session, symbol, depth=3)

            if network:
                for level, peers_set in network.items():
                    print(f"  Level {level}: {len(peers_set)} peers")
                    # Show a few examples
                    examples = list(peers_set)[:5]
                    print(f"    Examples: {', '.join(examples)}")
            else:
                print(f"  No peer network found")


def test_peer_counts():
    """Test peer count queries"""
    print("\n" + "="*80)
    print("TEST 4: Peer Counts for Multiple Symbols")
    print("="*80)

    with get_session() as session:
        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'JPM', 'BAC']

        counts = get_peer_counts(session, test_symbols)

        print("\nPeer counts:")
        for symbol, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {symbol}: {count} peers")


def test_most_connected():
    """Test finding most connected symbols"""
    print("\n" + "="*80)
    print("TEST 5: Most Connected Symbols (Top 10)")
    print("="*80)

    with get_session() as session:
        top_symbols = find_most_connected(session, limit=10)

        print("\nSymbols with most peers:")
        for i, item in enumerate(top_symbols, 1):
            print(f"  {i}. {item['symbol']}: {item['peer_count']} peers")


def test_search_by_peer():
    """Test reverse lookup - find who lists a symbol as peer"""
    print("\n" + "="*80)
    print("TEST 6: Reverse Lookup (Who Lists AAPL as Peer?)")
    print("="*80)

    with get_session() as session:
        test_symbols = ['AAPL', 'JPM']

        for symbol in test_symbols:
            listings = search_by_peer(session, symbol)
            if listings:
                print(f"\n{symbol} is listed as peer by {len(listings)} symbols:")
                print(f"  {', '.join(listings[:15])}")  # Show first 15
                if len(listings) > 15:
                    print(f"  ... and {len(listings) - 15} more")
            else:
                print(f"\n{symbol} is not listed as peer by any symbols")


def test_raw_format():
    """Test raw comma-separated format"""
    print("\n" + "="*80)
    print("TEST 7: Raw Peers Format")
    print("="*80)

    with get_session() as session:
        test_symbols = ['AAPL', 'MSFT']

        for symbol in test_symbols:
            raw = get_peers_raw(session, symbol)
            if raw:
                print(f"\n{symbol} (raw format):")
                # Truncate if too long
                if len(raw) > 100:
                    print(f"  {raw[:100]}...")
                else:
                    print(f"  {raw}")
            else:
                print(f"\n{symbol}: No raw peers data")


def print_summary():
    """Print test summary and database statistics"""
    print("\n" + "="*80)
    print("DATABASE STATISTICS")
    print("="*80)

    with get_session() as session:
        from src.database.models import PeersBulk

        # Count total records
        total_count = session.query(PeersBulk).count()
        print(f"\nTotal symbols in peers_bulk: {total_count:,}")

        # Count symbols with peers
        with_peers = session.query(PeersBulk).filter(
            PeersBulk.peers_list.isnot(None),
            PeersBulk.peers_list != ''
        ).count()
        print(f"Symbols with peers: {with_peers:,}")
        print(f"Symbols without peers: {(total_count - with_peers):,}")

        # Get collection timestamp (sample from first record)
        sample = session.query(PeersBulk).first()
        if sample:
            print(f"Last collected: {sample.collected_at}")


def main():
    """Run all tests"""
    print("\n")
    print("="*80)
    print("BULK PEERS COLLECTION - TEST SUITE")
    print("="*80)

    try:
        # Run all test functions
        test_basic_queries()
        test_common_peers()
        test_peer_network()
        test_peer_counts()
        test_most_connected()
        test_search_by_peer()
        test_raw_format()

        # Print summary
        print_summary()

        print("\n" + "="*80)
        print("[SUCCESS] ALL TESTS COMPLETED")
        print("="*80 + "\n")

        return 0

    except Exception as e:
        print("\n" + "="*80)
        print("[ERROR] TEST FAILED")
        print("="*80)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

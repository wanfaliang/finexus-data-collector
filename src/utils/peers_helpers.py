"""
Peers Query Helper Functions
Utilities for querying stock peer relationships from bulk peers data
"""
from typing import Optional, List, Set, Dict
from sqlalchemy.orm import Session

from src.database.models import PeersBulk


def get_peers(session: Session, symbol: str) -> Optional[List[str]]:
    """
    Get list of peers for a symbol

    Args:
        session: Database session
        symbol: Stock symbol

    Returns:
        List of peer symbols or None if not found
    """
    result = session.query(PeersBulk).filter(
        PeersBulk.symbol == symbol
    ).first()

    if not result or not result.peers_list:
        return None

    # Split comma-separated list
    peers = [p.strip() for p in result.peers_list.split(',') if p.strip()]
    return peers if peers else None


def get_peers_raw(session: Session, symbol: str) -> Optional[str]:
    """
    Get raw comma-separated peers string for a symbol

    Args:
        session: Database session
        symbol: Stock symbol

    Returns:
        Comma-separated peers string or None if not found
    """
    result = session.query(PeersBulk).filter(
        PeersBulk.symbol == symbol
    ).first()

    return result.peers_list if result else None


def find_common_peers(
    session: Session,
    symbol1: str,
    symbol2: str
) -> List[str]:
    """
    Find common peers between two symbols

    Args:
        session: Database session
        symbol1: First stock symbol
        symbol2: Second stock symbol

    Returns:
        List of common peer symbols (may be empty)
    """
    peers1 = get_peers(session, symbol1)
    peers2 = get_peers(session, symbol2)

    if not peers1 or not peers2:
        return []

    common = set(peers1) & set(peers2)
    return sorted(list(common))


def are_peers(session: Session, symbol1: str, symbol2: str) -> bool:
    """
    Check if two symbols are listed as peers of each other

    Args:
        session: Database session
        symbol1: First stock symbol
        symbol2: Second stock symbol

    Returns:
        True if they are mutual peers
    """
    peers1 = get_peers(session, symbol1)
    peers2 = get_peers(session, symbol2)

    if not peers1 or not peers2:
        return False

    return symbol2 in peers1 and symbol1 in peers2


def get_peer_network(
    session: Session,
    symbol: str,
    depth: int = 1
) -> Dict[int, Set[str]]:
    """
    Get network of peers at different depths
    Depth 1: Direct peers
    Depth 2: Peers of peers
    Depth 3: Peers of peers of peers, etc.

    Args:
        session: Database session
        symbol: Starting stock symbol
        depth: How many levels deep to traverse (1-3 recommended)

    Returns:
        Dictionary mapping depth level to set of symbols at that level
        Example: {1: {'AAPL', 'MSFT'}, 2: {'GOOGL', 'AMZN', ...}}
    """
    if depth < 1:
        return {}

    network = {}
    visited = {symbol}  # Don't revisit the starting symbol
    current_level = {symbol}

    for level in range(1, depth + 1):
        next_level = set()

        for sym in current_level:
            peers = get_peers(session, sym)
            if peers:
                for peer in peers:
                    if peer not in visited:
                        next_level.add(peer)
                        visited.add(peer)

        if next_level:
            network[level] = next_level
            current_level = next_level
        else:
            break  # No more peers to explore

    return network


def get_peer_counts(session: Session, symbols: List[str]) -> Dict[str, int]:
    """
    Get peer count for multiple symbols

    Args:
        session: Database session
        symbols: List of stock symbols

    Returns:
        Dictionary mapping symbol to number of peers
    """
    results = session.query(PeersBulk).filter(
        PeersBulk.symbol.in_(symbols)
    ).all()

    counts = {}
    for result in results:
        if result.peers_list:
            peer_count = len([p for p in result.peers_list.split(',') if p.strip()])
            counts[result.symbol] = peer_count
        else:
            counts[result.symbol] = 0

    # Add zeros for symbols not found
    for symbol in symbols:
        if symbol not in counts:
            counts[symbol] = 0

    return counts


def find_most_connected(session: Session, limit: int = 10) -> List[Dict]:
    """
    Find symbols with the most peers (most connected)

    Args:
        session: Database session
        limit: Number of results to return

    Returns:
        List of dictionaries with symbol and peer count
    """
    results = session.query(PeersBulk).all()

    # Calculate peer counts
    symbol_counts = []
    for result in results:
        if result.peers_list:
            peer_count = len([p for p in result.peers_list.split(',') if p.strip()])
            symbol_counts.append({
                'symbol': result.symbol,
                'peer_count': peer_count,
                'peers': result.peers_list
            })

    # Sort by peer count descending
    symbol_counts.sort(key=lambda x: x['peer_count'], reverse=True)

    return symbol_counts[:limit]


def search_by_peer(session: Session, peer_symbol: str) -> List[str]:
    """
    Find all symbols that list a specific symbol as a peer

    Args:
        session: Database session
        peer_symbol: Symbol to search for in peers lists

    Returns:
        List of symbols that have peer_symbol in their peers list
    """
    # Use SQL LIKE to find symbol in comma-separated list
    results = session.query(PeersBulk).filter(
        PeersBulk.peers_list.contains(peer_symbol)
    ).all()

    matches = []
    for result in results:
        if result.peers_list:
            # Verify exact match (not substring)
            peers = [p.strip() for p in result.peers_list.split(',')]
            if peer_symbol in peers:
                matches.append(result.symbol)

    return sorted(matches)


if __name__ == "__main__":
    # Example usage
    from src.database.connection import get_session

    with get_session() as session:
        # Test get_peers
        symbol = 'AAPL'
        peers = get_peers(session, symbol)
        if peers:
            print(f"{symbol} peers: {peers}")
            print(f"Number of peers: {len(peers)}")
        else:
            print(f"No peers found for {symbol}")

        # Test common peers
        common = find_common_peers(session, 'AAPL', 'MSFT')
        if common:
            print(f"\nCommon peers between AAPL and MSFT: {common}")

        # Test peer network
        network = get_peer_network(session, 'AAPL', depth=2)
        print(f"\nPeer network for AAPL:")
        for level, peers_set in network.items():
            print(f"  Level {level}: {len(peers_set)} peers")

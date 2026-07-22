"""Unique Paths with clear typing - expressing path counts semantically."""

from typing import List, Literal
from functools import lru_cache

# Type aliases that express WHAT the values mean, not just their literal value
PathCount = int  # Represents: number of valid paths from a position to destination


class Solution:
    """Solve unique paths problem with proper type hints and semantics."""

    def uniquePathsWithObstacles(self, grid: List[List[int]]) -> int:
        """
        Find number of unique paths from top-left to bottom-right.
        - Cannot pass through obstacles (grid[i][j] == 1)
        - Can only move right or down

        Returns:
            PathCount: Total number of valid paths (0 if blocked, >= 1 if possible)
        """
        if not grid or grid[0][0] == 1:
            return 0

        rows = len(grid)
        cols = len(grid[0])

        @lru_cache(maxsize=None)
        def dfs(r: int, c: int) -> Literal[0, 1] | int:
            """
            Count valid paths from position (r, c) to bottom-right.

            Returns:
                - 0: No valid paths (hit obstacle or out of bounds)
                - 1: Reached destination (one complete path exists)
                - n: Number of valid paths through this cell
            """
            # Base case: out of bounds -> NO VALID PATHS
            if r >= rows or c >= cols:
                return 0  # 0 because path doesn't exist

            # Base case: hit obstacle -> NO VALID PATHS
            if grid[r][c] == 1:
                return 0  # 0 because path is blocked

            # Base case: reached destination -> ONE VALID PATH EXISTS
            if r == rows - 1 and c == cols - 1:
                return 1  # 1 because we found exactly one way to reach here

            # Recursive case: sum paths from right + paths from down
            paths_right = dfs(r, c + 1)  # Try moving right
            paths_down = dfs(r + 1, c)   # Try moving down
            return paths_right + paths_down

        return dfs(0, 0)


class SolutionWithExplicitSemantics:
    """Alternative: use named constants for semantic clarity."""

    # Named constants express the semantic meaning clearly
    NO_PATH_EXISTS = 0      # Base case: blocked or unreachable
    DESTINATION_REACHED = 1  # Base case: found the goal

    def uniquePathsWithObstacles(self, grid: List[List[int]]) -> int:
        """Find number of unique paths with explicit semantic constants."""
        if not grid or grid[0][0] == 1:
            return self.NO_PATH_EXISTS

        rows = len(grid)
        cols = len(grid[0])

        @lru_cache(maxsize=None)
        def dfs(r: int, c: int) -> int:
            """
            Count valid paths from (r, c) to destination.

            Returns:
                NO_PATH_EXISTS (0) when blocked
                DESTINATION_REACHED (1) when goal found
                Sum of subproblem results otherwise
            """
            # Out of bounds or obstacle -> NO valid paths
            if r >= rows or c >= cols or grid[r][c] == 1:
                return self.NO_PATH_EXISTS

            # Reached destination -> exactly ONE valid path
            if r == rows - 1 and c == cols - 1:
                return self.DESTINATION_REACHED

            # Sum all valid paths from this position
            return dfs(r, c + 1) + dfs(r + 1, c)

        return dfs(0, 0)


# Example usage
if __name__ == "__main__":
    test_cases = [
        [[0, 0, 0], [0, 1, 0], [0, 0, 0]],  # Expected: 2
        [[0, 1], [0, 0]],                    # Expected: 1
        [[1, 0]],                            # Expected: 0
    ]

    solution = Solution()
    for grid in test_cases:
        result = solution.uniquePathsWithObstacles(grid)
        print(f"Grid: {grid}")
        print(f"Unique paths: {result}\n")

import rubik

def shortest_path(start, end):
    """
    Using 2-way BFS, finds the shortest path from start_position to
    end_position. Returns a list of moves. 

    You can use the rubik.quarter_twists move set.
    Each move can be applied using rubik.perm_apply
    """
    if start == end:
        return []  
    forward = ([start], {start : []})  
    backward = ([end], {end : []})
    for iter in range(7):
        (forward_states, forward_moves) = forward
        (backward_states, backward_moves) = backward

        next_forward_states = []
        for state in forward_states:
            moves = forward_moves[state]
            for twist in rubik.quarter_twists:
                next_state = rubik.perm_apply(twist, state)  
                if next_state in backward_moves:
                    return moves + [twist] + backward_moves[next_state]
                if not next_state in forward_moves:
                    next_forward_states.append(next_state) 
                    forward_moves[next_state] = moves + [twist]

        next_backward_states = []
        for state in backward_states:
            moves = backward_moves[state]
            for twist in rubik.quarter_twists:
                next_state = rubik.perm_apply(rubik.perm_inverse(twist), state)  
                if next_state in forward_moves:
                    return forward_moves[next_state] + [twist] + moves 
                if not next_state in backward_moves:
                    next_backward_states.append(next_state) 
                    backward_moves[next_state] = [twist] + moves

        forward = (next_forward_states, forward_moves)
        backward = (next_backward_states, backward_moves)

    return None

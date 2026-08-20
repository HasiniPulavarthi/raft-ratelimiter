#pragma once
#include <string>

#include "raft/types.hpp"

namespace raft {

// Anything that can be driven by a replicated log implements this.
// Raft itself is completely agnostic to what `command` means -- it just
// guarantees every node applies the same commands in the same order.
class StateMachine {
public:
    virtual ~StateMachine() = default;
    virtual std::string Apply(Index index, const std::string& command) = 0;
};

}  // namespace raft


#ifndef PARQUET_RECORD_FILTER_H
#define PARQUET_RECORD_FILTER_H

#include <vector>

namespace parquet_cpp {
using std::vector;

enum expr_operator_t {
  OP_LEAF = 0,
  OP_NOT = 1,
  OP_OR = 2,
  OP_AND = 3
};

struct expr_node_t {
  expr_operator_t op_;
  vector<int> children_;
  int id_;

  expr_node_t(int id): op_(OP_LEAF), id_(id){}
};

class bool_expr_tree_t {
public:
  bool_expr_tree_t(vector<expr_node_t>& nodes)
  { nodes_.swap(nodes); }

  template<typename T>
  bool eval(int node_id, T& nodeset) const {
    const expr_node_t& n = nodes_[node_id];
    switch(n.op_) {
    case OP_LEAF: return nodeset.get(node_id);
    case OP_NOT: return ! eval(n.children_[0], nodeset);
    case OP_OR: {
      for(size_t i=0; i<n.children_.size(); ++i) {
        bool v = eval(n.children_[i], nodeset);
        if (v) return true;
      }
      return false;
    }
    case OP_AND: {
      for(size_t i=0; i<n.children_.size(); ++i) {
        bool v = eval(n.children_[i], nodeset);
        if (!v) return false;
      }
      return true;
    }
    default:
      return false;
    }
  }
private:
  vector<expr_node_t> nodes_;
};
}

#endif //PARQUET_RECORD_FILTER_H

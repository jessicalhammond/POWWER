import React, { Component } from 'react';
var GeminiScrollbar = require('react-gemini-scrollbar');
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import { FormattedMessage } from 'react-intl';
import s from './TodoList.css';
// import Translate from '../Translate';
// import $ from 'jquery';
// import PerfectScrollbar from 'perfect-scrollbar';
import TodoItem from './TodoItem';

// const todoData = [
//   { done: false, text: 'Meeting with Nabindar Singh' },
//   { done: false, text: 'Exercise at 6pm with Nicholas.' },
//   { done: false, text: 'Avengers Age of Ultron.' },
//   { done: false, text: 'Henna birthday at Mezbaan.' },
// ];

class TodoList extends Component {
  constructor(props) {
    super(props);
    this.state = {
      todoData: [{ done: false, text: 'Meeting with Nabindar Singh' },
      { done: false, text: 'Exercise at 6pm with Nicholas.' },
      { done: false, text: 'Avengers Age of Ultron.' },
      { done: false, text: 'Henna birthday at Mezbaan.' }],
    };
    this.addItem = this.addItem.bind(this);
  }
  addItem(e) {
    e.preventDefault();
    if (e.target[0].value !== '') {
      const newTodoList = this.state.todoData;
      newTodoList.push(
        {
          done: false,
          text: e.target[0].value,
        }
      );
      this.setState({
        todoData: newTodoList,
      });
      e.target[0].value = ''; // eslint-disable-line
    }
    return false;
  }
  render() {
    return (
      <div className={`todo-container panel panel-danger ${s.todoContainer}`}>
        <div className={`panel-heading ${s.panelHeading}`}>
          <div className={`todo-header text-center ${s.todoHeader}`}>
            <h4><i className={`fa fa-tasks ${s.fa}`} />&nbsp;
              <FormattedMessage id="todo" defaultMessage="To Do List" />
            </h4>
          </div>
        </div>
        <div className={`panel-body bg-danger ${s.panelBody}`}>
          <div>
            <div className={`todo-list-wrap ${s.todoListWrap}`}>
              <GeminiScrollbar>
                <ul className={`todo-list ${s.todoListItem}`}>
                  {this.state.todoData
                    .map((item, i) => <TodoItem key={i} done={item.done} text={item.text} />)
                  }
                </ul>
              </GeminiScrollbar>
            </div>
            <form
              className={`form-horizontal todo-from-bottom ${s.todoFormBottom}`}
              onSubmit={this.addItem}
            >
              <div className="row">
                <div className="col-sm-12">
                  <div className="input-group">
                    <input
                      type="text"
                      className={`form-control ${s.formControl}`}
                      placeholder="Add New Item"
                    />
                    <span className="input-group-btn">
                      <button type="submit" className={`btn btn-default ${s.btn}`}>
                        <FormattedMessage
                          id="add"
                          defaultMessage="Add"
                        />
                      </button>
                    </span>
                  </div>
                </div>
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(TodoList);

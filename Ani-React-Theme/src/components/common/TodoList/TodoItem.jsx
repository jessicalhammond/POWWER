import React, { Component } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './TodoList.css';

class TodoItem extends Component {
  constructor(props) {
    super(props);
    this.state = {
      done: false,
    };
    this.toggle = this.toggle.bind(this);
  }
  toggle() {
    const toggleValue = !this.state.done;
    this.setState({
      done: toggleValue,
    });
  }
  render() {
    return (
      <li>
        <label className={`checked1 animated bounceInRight ${s.checkboxWrap}`}>
          <input type="checkbox" onClick={(e) => { this.toggle(); }} className={s.checkboxStyle} />
          <span />
        </label>
        <span className={`done-false animated bounceInRight ${this.state.done ? s.doneTrue : s.doneFalse}`}>{this.props.text}</span>
      </li>
    );
  }
}

export default withStyles(s)(TodoItem);

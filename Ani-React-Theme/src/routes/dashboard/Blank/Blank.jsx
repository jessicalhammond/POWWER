import React, { Component, PropTypes } from 'react';

const title = 'Blank Page';

class Blank extends Component {

  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  componentWillMount() {
    this.context.setTitle(title);
  }
  render() {
    return (
      <div className="animate" />
    );
  }
}

export default Blank;



import React, { Component, PropTypes } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './Docs.css';
import Link from '../../components/Link';

import profilePage from '../../common/images/flat-avatar.png';

const title = 'Documentation';

class Docs extends Component {

  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  }

  componentWillMount() {
    this.context.setTitle(title);
  }

  render() {
    return (
      <div className={` animate ${s.DocsPage}`}>
        <div className="row">
          <div className="col-md-4 col-lg-4 col-md-offset-4 col-lg-offset-4">
            <Link to="/" className="hvr-pulse-grow">
              <img src={profilePage} className="user-avatar" alt="profile" />
            </Link>
            <h1>Ani Theme </h1>
            <div>
              <h3>Documentation Coming Soon.</h3>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(Docs);

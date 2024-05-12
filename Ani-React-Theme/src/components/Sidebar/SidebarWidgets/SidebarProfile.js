import React, { Component } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './SidebarWidgets.css';
// import Translate from '../../common/Translate';
import Link from '../../Link';
import profilePicture from '../../../common/images/flat-avatar.png';

class SidebarNewsFeed extends Component {
  render() {
    return (
      <div className="text-center">
        <Link to="/dashboard/profile">
          <img
            src={profilePicture}
            className={`user-avatar ${s.userAvatar}`}
            alt="user profile"
          />
        </Link>
        <div className={`text-center ${s.avatarName}`}>
          Ani Pascal
        </div>
      </div>
    );
  }
}

export default withStyles(s)(SidebarNewsFeed);

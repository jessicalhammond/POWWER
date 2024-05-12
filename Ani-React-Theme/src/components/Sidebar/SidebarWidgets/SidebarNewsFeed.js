import React, { Component } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import { FormattedMessage } from 'react-intl';
import s from './SidebarWidgets.css';
// import Translate from '../../common/Translate';

class SidebarNewsFeed extends Component { // eslint-disable-line
  render() {
    return (
      <div className={`news-feed ${s.newsFeed}`}>
        <div className={`feed-header ${s.feedHeader}`}>
          <FormattedMessage
            id="feed"
            defaultMessage="NEWS FEED"
          />
        </div>
        <div className={`feed-content ${s.feedContent}`}>
          <ul className="feed">
            <li>
              <a href="">
                <FormattedMessage
                  id="li1"
                  defaultMessage="Anna liked StrapUI - Dashboard Themes & Templates."
                />
              </a>
              <span className={`feed-date ${s.feedDate}`}>25/4/2015</span>
            </li>
            <li>
              <a href="">
                <FormattedMessage
                  id="li2"
                  defaultMessage="Henna birthday at Mezbaan at 7:00pm."
                />
              </a>
              <span className={`feed-date ${s.feedDate}`}>25/4/2015</span>
            </li>
            <li>
              <a href="">
                <FormattedMessage
                  id="li3"
                  defaultMessage="Animesh Saha commented on your post."
                />
              </a>
              <span className={`feed-date ${s.feedDate}`}>25/4/2015</span>
            </li>
            <li>
              <a href="">
                <FormattedMessage
                  id="li4"
                  defaultMessage="Server Alert: Need to upgrade your server."
                />
              </a>
              <span className={`feed-date ${s.feedDate}`}>25/4/2015</span>
            </li>
          </ul>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(SidebarNewsFeed);

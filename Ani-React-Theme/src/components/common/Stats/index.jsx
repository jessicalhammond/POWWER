import React, { Component } from 'react';
import { ProgressBar } from 'react-bootstrap';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './Stats.css';
import Link from '../../Link';

class Stats extends Component {
  render() {
    const iconClass = `fa fa-${this.props.icon} fa-4x text-${this.props.bgClass}`;
    return (
      <Link to={this.props.link} className={`stat hvr-wobble-horizontal ${s.stat}`}>
        <div className={`stat-icon ${s.statIcon}`}>
          <i className={iconClass} />
        </div>

        <div className={`stat-label ${s.statLabel}`}>
          <div className={`label-header ${s.labelHeader}`}>
            {this.props.value}
          </div>

          <ProgressBar
            bsStyle={this.props.bgClass}
            className={`progress-sm ${s.progressSm}`}
            now={this.props.progressValue}
            key={1}
          />
          <div className={`clearfix stat-detail ${s.statDetail}`}>
            <div className="label-body">
              <i className="fa fa-arrow-circle-o-right pull-right text-muted" />
              {this.props.text}
            </div>
          </div>
        </div>
      </Link>
    );
  }
}

export default withStyles(s)(Stats);

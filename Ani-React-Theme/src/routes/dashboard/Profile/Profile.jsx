import React, { Component, PropTypes } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import { Panel } from 'react-bootstrap';
import s from './Profile.css';
// import Link from '../../../components/Link';

import cover from '../../../common/images/profile-cover.jpg';
import profilePic from '../../../common/images/flat.png';
import flatAvatart from '../../../common/images/flat-avatar.png';
import colorful4 from '../../../common/images/colorful4.jpg';

const title = 'Profile';

class Profile extends Component {
  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  componentWillMount() {
    this.context.setTitle(title);
  }
  render() {
    return (
      <div className="animate">
        <div>
          <div className={`cover-wrapper ${s.coverWrapper}`}>
            <div
              className={`cover-photo ${s.coverPhoto}`}
              style={{ backgroundImage: `url(${cover})` }}
            >
              <div className={`name-desg ${s.nameDesg}`}>
                <h3>
                  Kumar Sanket
                  <small>CEO and Founder @Sahusoft</small>
                </h3>
              </div>
            </div>
            <div className={`profile-photo-warp ${s.profilePhotoWrapper}`}>
              <img
                className={`profile-photo img-responsive img-circle ${s.profilePhoto}`}
                src={profilePic} alt=""
              />
            </div>
            <div className={`foobar ${s.foobar}`}>
              <a href=""><i className={`fa fa-heart text-danger ${s.faFooBar}`} /> 443</a>
                &nbsp;&nbsp;&nbsp;
              <a href=""><i className={`fa fa-users ${s.faFooBar}`} /> 443</a>
              <span className={`probutton ${s.probutton}`}>
                <button
                  type="button"
                  className={`btn ${s.btnProButton} ${s.btnBordered} ${s.btnPrimary}`}
                >
                Follow
                </button>
              </span>
              <span className={`links pull-right ${s.links}`}>
                <a href=""><i className={`fa fa-twitter ${s.faFooBar} ${s.twitter}`} /></a>
                <a href=""><i className={`fa fa-facebook ${s.faFooBar} ${s.facebook}`} /></a>
                <a href=""><i className={`fa fa-google-plus ${s.faFooBar} ${s.googlePlus}`} /></a>
                <a href=""><i className={`fa fa-github ${s.faFooBar} ${s.github}`} /></a>
              </span>
            </div>
          </div>

          <div className={s.contentWrapper}>
            <div>
              <div className={`profile-body row ${s.profileBody}`} id="profile-items">
                <div className="col-sm-6">
                  <div className={`profile-comment prophoto ${s.profileComment} ${s.proPhoto}`}>
                    <Panel
                      bsStyle="default"
                      footer={
                        <div className={`clearfix ${s.panelFooter}`} >
                          <a href=""><i className={`fa fa-picture-o ${s.fa}`} /></a>
                          <a href=""><i className={`fa fa-calendar ${s.fa}`} /></a>
                          <a href=""><i className={`fa fa-video-camera ${s.fa}`} /></a>
                          <button
                            type="button"
                            className={`btn btn-primary pull-right ${s.btnPanel}`}
                          >
                            Send Message
                          </button>
                        </div>
                      }
                    >
                      <textarea name="" id="" cols="54" rows="4" />
                    </Panel>
                  </div>

                  <div className={`prophoto ${s.proPhoto}`}>
                    <Panel
                      bsStyle="default"
                      className={s.panelWrapper}
                      header={
                        <h3 className={`panel-title ${s.panelTitile}`}>
                          <img
                            className={`panel-photo img-responsive img-circle ${s.panelPhoto}`}
                            src={flatAvatart} alt=""
                          />
                        Kumar
                          <br />
                          <span className={`text-muted ${s.textMuted}`}>
                            Posted on 3rd March 2014
                          </span>
                        </h3>
                      }
                    >
                      <img className="img-responsive" src={colorful4} alt="" height="200" />
                      <div className={`comment-links clearfix ${s.commentLinks}`}>
                        <a href=""><i className="fa fa-share-alt" />22</a>
                        <a href=""><i className="fa fa-comments-o" />106</a>
                        <a href=""><i className="fa fa-heart text-danger" />862</a>
                      </div>
                    </Panel>
                  </div>
                </div>

                <div className="col-sm-6">
                  <div className={`prophoto ${s.proPhoto}`}>
                    <Panel
                      bsStyle="default"
                      className={s.panelWrapper}
                      header={
                        <h3 className={`panel-title ${s.panelTitile}`}>
                          <img
                            className={`panel-photo img-responsive img-circle ${s.panelPhoto}`}
                            src={flatAvatart} alt=""
                          />
                        Kumar
                          <br />
                          <span className={`text-muted ${s.textMuted}`}>
                            Posted on 3rd March 2014
                          </span>
                        </h3>
                      }
                    >
                      <div className={`lorem ${s.lorem}`}>
                        Lorem ipsum dolor sit amet, consectetur adipisicing elit. Aspernatur harum
                        aliquid tempore molestias nemo modi quas repellat. Accusantium praesentium,
                         cupiditate tempore culpa voluptate laboriosam itaque error iste accusamus
                         reprehenderit illum! Lorem ipsum dolor sit amet, consectetur adipisicing
                         elit. Est saepe voluptas, eligendi necessitatibus adipisci soluta, amet
                          magnam, rerum, iure minima fuga praesentium nobis veniam quisquam illum
                          repellat beatae. Consectetur, asperiores.
                      </div>
                      <div className={`comment-links clearfix ${s.commentLinks}`}>
                        <a href=""><i className="fa fa-share-alt" />22</a>
                        <a href=""><i className="fa fa-comments-o" />106</a>
                        <a href=""><i className="fa fa-heart text-danger" />862</a>
                      </div>
                      <div className={`comments-here media ${s.commentsHere}`}>
                        <a className="pull-left" href="">
                          <img
                            className="media-object img-circle img-responsive"
                            src={flatAvatart} alt="Media Object"
                          />
                        </a>
                        <div className="media-body">
                          <a href=""><h5 className="media-heading">Kumar Pratik</h5></a>
                          <span className={`timely pull-right text-muted ${s.timely}`}>
                             3 hours ago
                          </span>
                            Lorem ipsum dolor sit amet, consectetur adipisicing elit. Hic
                            repudiandae exercitationem provident nihil consectetur.
                          <div className={`comment-like ${s.commentsLikes}`}>
                            <a href=""><i className="fa fa-comments-o" />106</a>
                            <a href=""><i className="fa fa-heart text-danger" />862</a>
                          </div>
                        </div>
                      </div>
                    </Panel>
                  </div>
                </div>

                <div className="col-sm-6">
                  <div className={`prophoto ${s.proPhoto}`}>
                    <Panel
                      bsStyle="default"
                      className={s.panelWrapper}
                      header={
                        <h3 className={`panel-title ${s.panelTitile}`}>
                          <img
                            className={`panel-photo img-responsive img-circle ${s.panelPhoto}`}
                            src={flatAvatart} alt=""
                          />
                        Kumar
                          <br />
                          <span className={`text-muted ${s.textMuted}`}>
                            Posted on 3rd March 2014
                          </span>
                        </h3>
                      }
                    >
                      <img className="img-responsive" src={colorful4} alt="" height="200" />
                      <div className={`comment-links clearfix ${s.commentLinks}`}>
                        <a href=""><i className="fa fa-share-alt" />22</a>
                        <a href=""><i className="fa fa-comments-o" />106</a>
                        <a href=""><i className="fa fa-heart text-danger" />862</a>
                      </div>
                    </Panel>
                  </div>
                </div>

              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(Profile);
